/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "recovery.h"

#include <fcntl.h>

#include "log_io.h"
#include "fiber.h"
#include "tt_pthread.h"
#include "fio.h"
#include "errinj.h"

/*
 * Recovery subsystem
 * ------------------
 *
 * A facade of the recovery subsystem is struct recovery_state,
 * which is a singleton.
 *
 * Depending on the configuration, start-up parameters, the
 * actual task being performed, the recovery can be
 * in a different state.
 *
 * The main factors influencing recovery state are:
 * - temporal: whether or not the instance is just booting
 *   from a snapshot, is in 'local hot standby mode', or
 *   is already accepting requests
 * - topological: whether or not it is a master instance
 *   or a replica
 * - task based: whether it's a master process,
 *   snapshot saving process or a replication relay.
 *
 * Depending on the above factors, recovery can be in two main
 * operation modes: "read mode", recovering in-memory state
 * from existing data, and "write mode", i.e. recording on
 * disk changes of the in-memory state.
 *
 * Let's enumerate all possible distinct states of recovery:
 *
 * Read mode
 * ---------
 * IR - initial recovery, initiated right after server start:
 * reading data from the snapshot and existing WALs
 * and restoring the in-memory state
 * IRR - initial replication relay mode, reading data from
 * existing WALs (xlogs) and sending it to the client.
 *
 * HS - standby mode, entered once all existing WALs are read:
 * following the WAL directory for all changes done by the master
 * and updating the in-memory state
 * RR - replication relay, following the WAL directory for all
 * changes done by the master and sending them to the
 * replica
 *
 * Write mode
 * ----------
 * M - master mode, recording in-memory state changes in the WAL
 * R - replica mode, receiving changes from the master and
 * recording them in the WAL
 * S - snapshot mode, writing entire in-memory state to a compact
 * snapshot file.
 *
 * The following state transitions are possible/supported:
 *
 * recovery_init() -> IR | IRR # recover()
 * IR -> HS         # recovery_follow_local()
 * IRR -> RR        # recovery_follow_local()
 * HS -> M          # recovery_finalize()
 * M -> R           # recovery_follow_remote()
 * R -> M           # recovery_stop_remote()
 * M -> S           # snapshot()
 * R -> S           # snapshot()
 */

struct recovery_state *recovery_state;

static const uint64_t snapshot_cookie = 0;

const char *wal_mode_STRS[] = { "none", "write", "fsync", "fsync_delay", NULL };

/* {{{ LSN API */

void
wait_lsn_set(struct wait_lsn *wait_lsn, int64_t lsn)
{
	assert(wait_lsn->waiter == NULL);
	wait_lsn->waiter = fiber_ptr;
	wait_lsn->lsn = lsn;
}


/* Alert the waiter, if any. */
static inline void
wakeup_lsn_waiter(struct recovery_state *r)
{
	if (r->wait_lsn.waiter && r->confirmed_lsn >= r->wait_lsn.lsn) {
		fiber_wakeup(r->wait_lsn.waiter);
	}
}

void
confirm_lsn(struct recovery_state *r, int64_t lsn, bool is_commit)
{
	assert(r->confirmed_lsn <= r->lsn);

	if (r->confirmed_lsn < lsn) {
		if (is_commit) {
			if (r->confirmed_lsn + 1 != lsn)
				say_warn("non consecutive LSN, confirmed: %jd, "
					 " new: %jd, diff: %jd",
					 (intmax_t) r->confirmed_lsn,
					 (intmax_t) lsn,
					 (intmax_t) (lsn - r->confirmed_lsn));
			r->confirmed_lsn = lsn;
		 }
	} else {
		 /*
		 * There can be holes in
		 * confirmed_lsn, in case of disk write failure, but
		 * wal_writer never confirms LSNs out order.
		 */
		assert(false);
		say_error("LSN is used twice or COMMIT order is broken: "
			  "confirmed: %jd, new: %jd",
			  (intmax_t) r->confirmed_lsn, (intmax_t) lsn);
	}
	wakeup_lsn_waiter(r);
}

void
set_lsn(struct recovery_state *r, int64_t lsn)
{
	r->lsn = lsn;
	r->confirmed_lsn = lsn;
	say_debug("set_lsn(%p, %" PRIi64, r, r->lsn);
	wakeup_lsn_waiter(r);
}

/** Wait until the given LSN makes its way to disk. */
void
recovery_wait_lsn(struct recovery_state *r, int64_t lsn)
{
	while (lsn < r->confirmed_lsn) {
		wait_lsn_set(&r->wait_lsn, lsn);
		try {
			fiber_yield();
			wait_lsn_clear(&r->wait_lsn);
		} catch (const Exception& e) {
			wait_lsn_clear(&r->wait_lsn);
			throw;
		}
	}
}


int64_t
next_lsn(struct recovery_state *r)
{
	r->lsn++;
	say_debug("next_lsn(%p, %" PRIi64, r, r->lsn);
	return r->lsn;
}


/* }}} */

/* {{{ Initial recovery */

static int
wal_writer_start(struct recovery_state *state);
void
wal_writer_stop(struct recovery_state *r);
static void
recovery_stop_local(struct recovery_state *r);

void
recovery_init(const char *snap_dirname, const char *wal_dirname,
	      row_handler row_handler, void *row_handler_param,
	      int rows_per_wal, int flags)
{
	assert(recovery_state == NULL);
	recovery_state = (struct recovery_state *) p0alloc(eter_pool, sizeof(struct recovery_state));
	struct recovery_state *r = recovery_state;
	recovery_update_mode(r, "none", 0);

	assert(rows_per_wal > 1);

	r->row_handler = row_handler;
	r->row_handler_param = row_handler_param;

	r->snap_dir = &snap_dir;
	r->snap_dir->dirname = strdup(snap_dirname);
	r->wal_dir = &wal_dir;
	r->wal_dir->dirname = strdup(wal_dirname);
	if (r->wal_mode == WAL_FSYNC) {
		(void) strcat(r->wal_dir->open_wflags, "s");
	}
	r->rows_per_wal = rows_per_wal;
	wait_lsn_clear(&r->wait_lsn);
	r->flags = flags;
}

void
recovery_update_mode(struct recovery_state *r,
		     const char *mode, double fsync_delay)
{
	r->wal_mode = (enum wal_mode) strindex(wal_mode_STRS, mode, WAL_MODE_MAX);
	assert(r->wal_mode != WAL_MODE_MAX);
	/* No mutex lock: let's not bother with whether
	 * or not a WAL writer thread is present, and
	 * if it's present, the delay will be propagated
	 * to it whenever there is a next lock/unlock of
	 * wal_writer->mutex.
	 */
	r->wal_fsync_delay = fsync_delay;
}

void
recovery_update_io_rate_limit(struct recovery_state *r, double new_limit)
{
	r->snap_io_rate_limit = new_limit * 1024 * 1024;
	if (r->snap_io_rate_limit == 0)
		r->snap_io_rate_limit = UINT64_MAX;
}

void
recovery_free()
{
	struct recovery_state *r = recovery_state;
	if (r == NULL)
		return;

	if (r->watcher)
		recovery_stop_local(r);

	if (r->writer)
		wal_writer_stop(r);

	free(r->snap_dir->dirname);
	free(r->wal_dir->dirname);
	if (r->current_wal) {
		/*
		 * Possible if shutting down a replication
		 * relay or if error during startup.
		 */
		log_io_close(&r->current_wal);
	}

	recovery_state = NULL;
}

void
recovery_setup_panic(struct recovery_state *r, bool on_snap_error, bool on_wal_error)
{
	r->wal_dir->panic_if_error = on_wal_error;
	r->snap_dir->panic_if_error = on_snap_error;
}


/**
 * Read a snapshot and call row_handler for every snapshot row.
 * Panic in case of error.
 */
void
recover_snap(struct recovery_state *r)
{
	/*  current_wal isn't open during initial recover. */
	assert(r->current_wal == NULL);
	say_info("recovery start");

	struct log_io *snap;
	int64_t lsn;

	lsn = greatest_lsn(r->snap_dir);
	if (lsn <= 0) {
		say_error("can't find snapshot");
		goto error;
	}
	snap = log_io_open_for_read(r->snap_dir, lsn, NONE);
	if (snap == NULL) {
		say_error("can't find/open snapshot");
		goto error;
	}
	say_info("recover from `%s'", snap->filename);
	struct log_io_cursor i;

	log_io_cursor_open(&i, snap);

	const char *row;
	uint32_t rowlen;
	while ((row = log_io_cursor_next(&i, &rowlen))) {
		if (r->row_handler(r->row_handler_param, row, rowlen) < 0) {
			say_error("can't apply row");
			if (snap->dir->panic_if_error)
				break;
		}
	}
	log_io_cursor_close(&i);
	log_io_close(&snap);

	if (row == NULL) {
		r->lsn = r->confirmed_lsn = lsn;
		say_info("snapshot recovered, confirmed lsn: %"
			 PRIi64, r->confirmed_lsn);
		return;
	}
error:
	if (greatest_lsn(r->snap_dir) <= 0) {
		say_crit("didn't you forget to initialize storage with --init-storage switch?");
		_exit(1);
	}
	panic("snapshot recovery failed");
}

#define LOG_EOF 0

/**
 * @retval -1 error
 * @retval 0 EOF
 * @retval 1 ok, maybe read something
 */
static int
recover_wal(struct recovery_state *r, struct log_io *l)
{
	int res = -1;
	struct log_io_cursor i;

	log_io_cursor_open(&i, l);

	const char *row;
	uint32_t rowlen;
	while ((row = log_io_cursor_next(&i, &rowlen))) {
		int64_t lsn = header_v11(row)->lsn;
		if (lsn <= r->confirmed_lsn) {
			say_debug("skipping too young row");
			continue;
		}
		/*
		 * After handler(row) returned, row may be
		 * modified, do not use it.
		 */
		if (r->row_handler(r->row_handler_param, row, rowlen) < 0) {
			say_error("can't apply row");
			if (l->dir->panic_if_error)
				goto end;
		}
		set_lsn(r, lsn);
	}
	res = i.eof_read ? LOG_EOF : 1;
end:
	log_io_cursor_close(&i);
	/* Sic: we don't close the log here. */
	return res;
}

/** Find out if there are new .xlog files since the current
 * LSN, and read them all up.
 *
 * This function will not close r->current_wal if
 * recovery was successful.
 */
static int
recover_remaining_wals(struct recovery_state *r)
{
	int result = 0;
	struct log_io *next_wal;
	int64_t current_lsn, wal_greatest_lsn;
	size_t rows_before;
	FILE *f;
	char *filename;
	enum log_suffix suffix;

	current_lsn = r->confirmed_lsn + 1;
	wal_greatest_lsn = greatest_lsn(r->wal_dir);

	/* if the caller already opened WAL for us, recover from it first */
	if (r->current_wal != NULL)
		goto recover_current_wal;

	while (current_lsn <= wal_greatest_lsn) {
		/*
		 * If a newer WAL appeared in the directory before
		 * current_wal was fully read, try re-reading
		 * one last time. */
		if (r->current_wal != NULL) {
			if (r->current_wal->retry++ < 3) {
				say_warn("`%s' has no EOF marker, yet a newer WAL file exists:"
					 " trying to re-read (attempt #%d)",
					 r->current_wal->filename, r->current_wal->retry);
				goto recover_current_wal;
			} else {
				say_warn("WAL `%s' wasn't correctly closed",
					 r->current_wal->filename);
				log_io_close(&r->current_wal);
			}
		}

		/*
		 * For the last WAL, first try to open .inprogress
		 * file: if it doesn't exist, we can safely try an
		 * .xlog, with no risk of a concurrent
		 * inprogress_log_rename().
		 */
		f = NULL;
		suffix = INPROGRESS;
		if (current_lsn == wal_greatest_lsn) {
			/* Last WAL present at the time of rescan. */
			filename = format_filename(r->wal_dir,
						   current_lsn, suffix);
			f = fopen(filename, "r");
		}
		if (f == NULL) {
			suffix = NONE;
			filename = format_filename(r->wal_dir,
						   current_lsn, suffix);
			f = fopen(filename, "r");
		}
		next_wal = log_io_open(r->wal_dir, LOG_READ, filename, suffix, f);
		/*
		 * When doing final recovery, and dealing with the
		 * last file, try opening .<ext>.inprogress.
		 */
		if (next_wal == NULL) {
			if (r->finalize && suffix == INPROGRESS) {
				/*
				 * There is an .inprogress file, but
				 * we failed to open it. Try to
				 * delete it.
				 */
				say_warn("unlink broken %s WAL", filename);
				if (inprogress_log_unlink(filename) != 0)
					panic("can't unlink 'inprogres' WAL");
				result = 0;
				break;
			}
			/* Missing xlog or gap in LSN */
			say_error("not all WALs have been successfully read");
			if (!r->wal_dir->panic_if_error) {
				/* Ignore missing WALs */
				say_warn("ignoring missing WALs");
				current_lsn++;
				continue;
			}
			result = -1;
			break;
		}
		assert(r->current_wal == NULL);
		r->current_wal = next_wal;
		say_info("recover from `%s'", r->current_wal->filename);

recover_current_wal:
		rows_before = r->current_wal->rows;
		result = recover_wal(r, r->current_wal);
		if (result < 0) {
			say_error("failure reading from %s",
				  r->current_wal->filename);
			break;
		}

		if (r->current_wal->rows > 0 &&
		    r->current_wal->rows != rows_before) {
			r->current_wal->retry = 0;
		}
		/* rows == 0 could indicate an empty WAL */
		if (r->current_wal->rows == 0) {
			say_error("read zero records from %s",
				  r->current_wal->filename);
			break;
		}
		if (result == LOG_EOF) {
			say_info("done `%s' confirmed_lsn: %" PRIi64,
				 r->current_wal->filename,
				 r->confirmed_lsn);
			log_io_close(&r->current_wal);
		}

		current_lsn = r->confirmed_lsn + 1;
	}

	/*
	 * It's not a fatal error when last WAL is empty, but if
	 * we lose some logs it is a fatal error.
	 */
	if (wal_greatest_lsn > r->confirmed_lsn + 1) {
		say_error("can't recover WALs");
		result = -1;
	}

	prelease(fiber_ptr->gc_pool);
	return result;
}

/**
 * Recover all WALs created after the last snapshot. Panic if
 * error.
 */
void
recover_existing_wals(struct recovery_state *r)
{
	int64_t next_lsn = r->confirmed_lsn + 1;
	int64_t wal_lsn = find_including_file(r->wal_dir, next_lsn);
	if (wal_lsn <= 0) {
		/* No WALs to recover from. */
		goto out;
	}
	if (log_io_open_for_read(r->wal_dir, wal_lsn, NONE) == NULL)
		goto out;
	if (recover_remaining_wals(r) < 0)
		panic("recover failed");
	say_info("WALs recovered, confirmed lsn: %" PRIi64, r->confirmed_lsn);
out:
	prelease(fiber_ptr->gc_pool);
}

void
recovery_finalize(struct recovery_state *r)
{
	int result;

	if (r->watcher)
		recovery_stop_local(r);

	r->finalize = true;

	result = recover_remaining_wals(r);
	if (result < 0)
		panic("unable to successfully finalize recovery");

	if (r->current_wal != NULL && result != LOG_EOF) {
		say_warn("WAL `%s' wasn't correctly closed", r->current_wal->filename);

		if (!r->current_wal->is_inprogress) {
			if (r->current_wal->rows == 0)
			        /* Regular WAL (not inprogress) must contain at least one row */
				panic("zero rows was successfully read from last WAL `%s'",
				      r->current_wal->filename);
		} else if (r->current_wal->rows == 0) {
			/* Unlink empty inprogress WAL */
			say_warn("unlink broken %s WAL", r->current_wal->filename);
			if (inprogress_log_unlink(r->current_wal->filename) != 0)
				panic("can't unlink 'inprogress' WAL");
		} else if (r->current_wal->rows == 1) {
			/* Rename inprogress wal with one row */
			say_warn("rename unfinished %s WAL", r->current_wal->filename);
			if (inprogress_log_rename(r->current_wal) != 0)
				panic("can't rename 'inprogress' WAL");
		} else
			panic("too many rows in inprogress WAL `%s'", r->current_wal->filename);

		log_io_close(&r->current_wal);
	}

	if ((r->flags & RECOVER_READONLY) == 0)
		wal_writer_start(r);
}


/* }}} */

/* {{{ Local recovery: support of hot standby and replication relay */

/**
 * This is used in local hot standby or replication
 * relay mode: look for changes in the wal_dir and apply them
 * locally or send to the replica.
 */
struct wal_watcher {
	/**
	 * Rescan the WAL directory in search for new WAL files
	 * every wal_dir_rescan_delay seconds.
	 */
	ev_timer dir_timer;
	/**
	 * When the latest WAL does not contain a EOF marker,
	 * re-read its tail on every change in file metadata.
	 */
	ev_stat stat;
	/** Path to the file being watched with 'stat'. */
	char filename[PATH_MAX+1];
};

static struct wal_watcher wal_watcher;

static void recovery_rescan_file(ev_stat *w, int revents __attribute__((unused)));

static void
recovery_watch_file(struct wal_watcher *watcher, struct log_io *wal)
{
	strncpy(watcher->filename, wal->filename, PATH_MAX);
	ev_stat_init(&watcher->stat, recovery_rescan_file, watcher->filename, 0.);
	ev_stat_start(&watcher->stat);
}

static void
recovery_stop_file(struct wal_watcher *watcher)
{
	ev_stat_stop(&watcher->stat);
}

static void
recovery_rescan_dir(ev_timer *w, int revents __attribute__((unused)))
{
	struct recovery_state *r = (struct recovery_state *) w->data;
	struct wal_watcher *watcher = r->watcher;
	struct log_io *save_current_wal = r->current_wal;

	int result = recover_remaining_wals(r);
	if (result < 0)
		panic("recover failed: %i", result);
	if (save_current_wal != r->current_wal) {
		if (save_current_wal != NULL)
			recovery_stop_file(watcher);
		if (r->current_wal != NULL)
			recovery_watch_file(watcher, r->current_wal);
	}
}

static void
recovery_rescan_file(ev_stat *w, int revents __attribute__((unused)))
{
	struct recovery_state *r = (struct recovery_state *) w->data;
	struct wal_watcher *watcher = r->watcher;
	int result = recover_wal(r, r->current_wal);
	if (result < 0)
		panic("recover failed");
	if (result == LOG_EOF) {
		say_info("done `%s' confirmed_lsn: %" PRIi64,
			 r->current_wal->filename,
			 r->confirmed_lsn);
		log_io_close(&r->current_wal);
		recovery_stop_file(watcher);
		/* Don't wait for wal_dir_rescan_delay. */
		recovery_rescan_dir(&watcher->dir_timer, 0);
	}
}

void
recovery_follow_local(struct recovery_state *r, ev_tstamp wal_dir_rescan_delay)
{
	assert(r->watcher == NULL);
	assert(r->writer == NULL);

	struct wal_watcher  *watcher = r->watcher= &wal_watcher;

	ev_timer_init(&watcher->dir_timer, recovery_rescan_dir,
		      wal_dir_rescan_delay, wal_dir_rescan_delay);
	watcher->dir_timer.data = watcher->stat.data = r;
	ev_timer_start(&watcher->dir_timer);
	/*
	 * recover() leaves the current wal open if it has no
	 * EOF marker.
	 */
	if (r->current_wal != NULL)
		recovery_watch_file(watcher, r->current_wal);
}

static void
recovery_stop_local(struct recovery_state *r)
{
	struct wal_watcher *watcher = r->watcher;
	assert(ev_is_active(&watcher->dir_timer));
	ev_timer_stop(&watcher->dir_timer);
	if (ev_is_active(&watcher->stat))
		ev_stat_stop(&watcher->stat);

	r->watcher = NULL;
}

/* }}} */

/* {{{ WAL writer - maintain a Write Ahead Log for every change
 * in the data state.
 */

struct wal_write_request {
	STAILQ_ENTRY(wal_write_request) wal_fifo_entry;
	/* Auxiliary. */
	int res;
	struct fiber *fiber;
	struct row_v11 row;
};

/* Context of the WAL writer thread. */
STAILQ_HEAD(wal_fifo, wal_write_request);

struct wal_writer
{
	struct wal_fifo input;
	struct wal_fifo commit;
	pthread_t thread;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	ev_async write_event;
	struct fio_batch *batch;
	bool is_shutdown;
	bool is_rollback;
};

static pthread_once_t wal_writer_once = PTHREAD_ONCE_INIT;

static struct wal_writer wal_writer;

/**
 * A pthread_atfork() callback for a child process. Today we only
 * fork the master process to save a snapshot, and in the child
 * the WAL writer thread is not necessary and not present.
 */
static void
wal_writer_child()
{
	log_io_atfork(&recovery_state->current_wal);
	if (wal_writer.batch) {
		free(wal_writer.batch);
		wal_writer.batch = NULL;
	}
	/*
	 * Make sure that atexit() handlers in the child do
	 * not try to stop the non-existent thread.
	 * The writer is not used in the child.
	 */
	recovery_state->writer = NULL;
}

/**
 * Today a WAL writer is started once at start of the
 * server.  Nevertheless, use pthread_once() to make
 * sure we can start/stop the writer many times.
 */
static void
wal_writer_init_once()
{
	(void) tt_pthread_atfork(NULL, NULL, wal_writer_child);
}

/**
 * A commit watcher callback is invoked whenever there
 * are requests in wal_writer->commit. This callback is
 * associated with an internal WAL writer watcher and is
 * invoked in the front-end main event loop.
 *
 * A rollback watcher callback is invoked only when there is
 * a rollback request and commit is empty.
 * We roll back the entire input queue.
 *
 * ev_async, under the hood, is a simple pipe. The WAL
 * writer thread writes to that pipe whenever it's done
 * handling a pack of requests (look for ev_async_send()
 * call in the writer thread loop).
 */
static void
wal_schedule_queue(struct wal_fifo *queue)
{
	/*
	 * Can't use STAILQ_FOREACH since fiber_call()
	 * destroys the list entry.
	 */
	struct wal_write_request *req, *tmp;
	STAILQ_FOREACH_SAFE(req, queue, wal_fifo_entry, tmp)
		fiber_call(req->fiber);
}

static void
wal_schedule(ev_async *watcher, int event __attribute__((unused)))
{
	struct wal_writer *writer = (struct wal_writer *) watcher->data;
	struct wal_fifo commit = STAILQ_HEAD_INITIALIZER(commit);
	struct wal_fifo rollback = STAILQ_HEAD_INITIALIZER(rollback);

	(void) tt_pthread_mutex_lock(&writer->mutex);
	STAILQ_CONCAT(&commit, &writer->commit);
	if (writer->is_rollback) {
		STAILQ_CONCAT(&rollback, &writer->input);
		writer->is_rollback = false;
	}
	(void) tt_pthread_mutex_unlock(&writer->mutex);

	wal_schedule_queue(&commit);
	/*
	 * Perform a cascading abort of all transactions which
	 * depend on the transaction which failed to get written
	 * to the write ahead log. Abort transactions
	 * in reverse order, performing a playback of the
	 * in-memory database state.
	 */
	STAILQ_REVERSE(&rollback, wal_write_request, wal_fifo_entry);
	wal_schedule_queue(&rollback);
}

/**
 * Initialize WAL writer context. Even though it's a singleton,
 * encapsulate the details just in case we may use
 * more writers in the future.
 */
static void
wal_writer_init(struct wal_writer *writer)
{
	/* I. Initialize the state. */
	pthread_mutexattr_t errorcheck;

	(void) tt_pthread_mutexattr_init(&errorcheck);

#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&writer->mutex, &errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);

	(void) tt_pthread_cond_init(&writer->cond, NULL);

	STAILQ_INIT(&writer->input);
	STAILQ_INIT(&writer->commit);

	ev_async_init(&writer->write_event, wal_schedule);
	writer->write_event.data = writer;

	(void) tt_pthread_once(&wal_writer_once, wal_writer_init_once);

	writer->batch = fio_batch_alloc(sysconf(_SC_IOV_MAX));

	if (writer->batch == NULL)
		panic_syserror("fio_batch_alloc");
}

/** Destroy a WAL writer structure. */
static void
wal_writer_destroy(struct wal_writer *writer)
{
	(void) tt_pthread_mutex_destroy(&writer->mutex);
	(void) tt_pthread_cond_destroy(&writer->cond);
	free(writer->batch);
}

/** WAL writer thread routine. */
static void *wal_writer_thread(void *worker_args);

/**
 * Initialize WAL writer, start the thread.
 *
 * @pre   The server has completed recovery from a snapshot
 *        and/or existing WALs. All WALs opened in read-only
 *        mode are closed.
 *
 * @param state			WAL writer meta-data.
 *
 * @return 0 success, -1 on error. On success, recovery->writer
 *         points to a newly created WAL writer.
 */
static int
wal_writer_start(struct recovery_state *r)
{
	assert(r->writer == NULL);
	assert(r->watcher == NULL);
	assert(r->current_wal == NULL);
	assert(! wal_writer.is_shutdown);
	assert(STAILQ_EMPTY(&wal_writer.input));
	assert(STAILQ_EMPTY(&wal_writer.commit));

	/* I. Initialize the state. */
	wal_writer_init(&wal_writer);
	r->writer = &wal_writer;

	ev_async_start(&wal_writer.write_event);

	/* II. Start the thread. */

	if (tt_pthread_create(&wal_writer.thread, NULL, wal_writer_thread, r)) {
		wal_writer_destroy(&wal_writer);
		r->writer = NULL;
		return -1;
	}
	return 0;
}

/** Stop and destroy the writer thread (at shutdown). */
void
wal_writer_stop(struct recovery_state *r)
{
	struct wal_writer *writer = r->writer;

	/* Stop the worker thread. */

	(void) tt_pthread_mutex_lock(&writer->mutex);
	writer->is_shutdown= true;
	(void) tt_pthread_cond_signal(&writer->cond);
	(void) tt_pthread_mutex_unlock(&writer->mutex);

	if (tt_pthread_join(writer->thread, NULL) != 0) {
		/* We can't recover from this in any reasonable way. */
		panic_syserror("WAL writer: thread join failed");
	}

	ev_async_stop(&writer->write_event);
	wal_writer_destroy(writer);

	r->writer = NULL;
}

/**
 * Pop a bulk of requests to write to disk to process.
 * Block on the condition only if we have no other work to
 * do. Loop in case of a spurious wakeup.
 */
void
wal_writer_pop(struct wal_writer *writer, struct wal_fifo *input)
{
	while (! writer->is_shutdown)
	{
		if (! writer->is_rollback && ! STAILQ_EMPTY(&writer->input)) {
			STAILQ_CONCAT(input, &writer->input);
			break;
		}
		(void) tt_pthread_cond_wait(&writer->cond, &writer->mutex);
	}
}

/**
 * If there is no current WAL, try to open it, and close the
 * previous WAL. We close the previous WAL only after opening
 * a new one to smoothly move local hot standby and replication
 * over to the next WAL.
 * If the current WAL has only 1 record, it means we need to
 * rename it from '.inprogress' to '.xlog'. We maintain
 * '.inprogress' WALs to ensure that, at any point in time,
 * an .xlog file contains at least 1 valid record.
 * In case of error, we try to close any open WALs.
 *
 * @post r->current_wal is in a good shape for writes or is NULL.
 * @return 0 in case of success, -1 on error.
 */
static int
wal_opt_rotate(struct log_io **wal, int rows_per_wal, struct log_dir *dir,
	       int64_t lsn)
{
	struct log_io *l = *wal, *wal_to_close = NULL;

	ERROR_INJECT_RETURN(ERRINJ_WAL_ROTATE);

	if (l != NULL && (l->rows >= rows_per_wal || lsn % rows_per_wal == 0)) {
		/*
		 * if l->rows == 1, log_io_close() does
		 * inprogress_log_rename() for us.
		 */
		wal_to_close = l;
		l = NULL;
	}
	if (l == NULL) {
		/* Open WAL with '.inprogress' suffix. */
		l = log_io_open_for_write(dir, lsn, INPROGRESS);
		/*
		 * Close the file *after* we create the new WAL, since
		 * this is when replication relays get an inotify alarm
		 * (when we close the file), and try to reopen the next
		 * WAL. In other words, make sure that replication relays
		 * try to open the next WAL only when it exists.
		 */
		if (wal_to_close) {
			/*
			 * We can not handle log_io_close()
			 * failure in any reasonable way.
			 * A warning is written to the server
			 * log file.
			 */
			log_io_close(&wal_to_close);
		}
	} else if (l->rows == 1) {
		/*
		 * Rename WAL after the first successful write
		 * to a name  without .inprogress suffix.
		 */
		if (inprogress_log_rename(l))
			log_io_close(&l);       /* error. */
	}
	assert(wal_to_close == NULL);
	*wal = l;
	return l ? 0 : -1;
}

static void
wal_opt_sync(struct log_io *wal, double sync_delay)
{
	static ev_tstamp last_sync = 0;

	if (sync_delay > 0 && ev_now() - last_sync >= sync_delay) {
		/*
		 * XXX: in case of error, we don't really know how
		 * many records were not written to disk: probably
		 * way more than the last one.
		 */
		(void) log_io_sync(wal);
		last_sync = ev_now();
	}
}

static struct wal_write_request *
wal_fill_batch(struct log_io *wal, struct fio_batch *batch, int rows_per_wal,
	       struct wal_write_request *req)
{
	int max_rows = wal->is_inprogress ? 1 : rows_per_wal - wal->rows;
	/* Post-condition of successful wal_opt_rotate(). */
	assert(max_rows > 0);
	fio_batch_start(batch, max_rows);
	while (req != NULL && ! fio_batch_is_full(batch)) {
		struct row_v11 *row = &req->row;
		header_v11_sign(&row->header);
		fio_batch_add(batch, row, row_v11_size(row));
		req = STAILQ_NEXT(req, wal_fifo_entry);
	}
	return req;
}

static struct wal_write_request *
wal_write_batch(struct log_io *wal, struct fio_batch *batch,
		struct wal_write_request *req, struct wal_write_request *end)
{
	int rows_written = fio_batch_write(batch, fileno(wal->f));
	wal->rows += rows_written;
	while (req != end && rows_written-- != 0)  {
		req->res = 0;
		req = STAILQ_NEXT(req, wal_fifo_entry);
	}
	return req;
}

static void
wal_write_to_disk(struct recovery_state *r, struct wal_writer *writer,
		  struct wal_fifo *input, struct wal_fifo *commit,
		  struct wal_fifo *rollback)
{
	struct log_io **wal = &r->current_wal;
	struct fio_batch *batch = writer->batch;

	struct wal_write_request *req = STAILQ_FIRST(input);
	struct wal_write_request *write_end = req;

	while (req) {
		if (wal_opt_rotate(wal, r->rows_per_wal, r->wal_dir,
				   req->row.header.lsn) != 0)
			break;
		struct wal_write_request *batch_end;
		batch_end = wal_fill_batch(*wal, batch, r->rows_per_wal, req);
		write_end = wal_write_batch(*wal, batch, req, batch_end);
		if (batch_end != write_end)
			break;
		wal_opt_sync(*wal, r->wal_fsync_delay);
		req = write_end;
	}
	STAILQ_SPLICE(input, write_end, wal_fifo_entry, rollback);
	STAILQ_CONCAT(commit, input);
}

/** WAL writer thread main loop.  */
static void *
wal_writer_thread(void *worker_args)
{
	struct recovery_state *r = (struct recovery_state *) worker_args;
	struct wal_writer *writer = r->writer;
	struct wal_fifo input = STAILQ_HEAD_INITIALIZER(input);
	struct wal_fifo commit = STAILQ_HEAD_INITIALIZER(commit);
	struct wal_fifo rollback = STAILQ_HEAD_INITIALIZER(rollback);

	(void) tt_pthread_mutex_lock(&writer->mutex);
	while (! writer->is_shutdown) {
		wal_writer_pop(writer, &input);
		(void) tt_pthread_mutex_unlock(&writer->mutex);

		wal_write_to_disk(r, writer, &input, &commit, &rollback);

		(void) tt_pthread_mutex_lock(&writer->mutex);
		STAILQ_CONCAT(&writer->commit, &commit);
		if (! STAILQ_EMPTY(&rollback)) {
			/*
			 * Begin rollback: create a rollback queue
			 * from all requests which were not
			 * written to disk and all requests in the
			 * input queue.
			 */
			writer->is_rollback = true;
			STAILQ_CONCAT(&rollback, &writer->input);
			STAILQ_CONCAT(&writer->input, &rollback);
		}
		ev_async_send(&writer->write_event);
	}
	(void) tt_pthread_mutex_unlock(&writer->mutex);
	if (r->current_wal != NULL)
		log_io_close(&r->current_wal);
	return NULL;
}

/**
 * WAL writer main entry point: queue a single request
 * to be written to disk and wait until this task is completed.
 */
int
wal_write(struct recovery_state *r, int64_t lsn, uint64_t cookie,
	  uint16_t op, const char *row, uint32_t row_len)
{
	say_debug("wal_write lsn=%" PRIi64, lsn);
	ERROR_INJECT_RETURN(ERRINJ_WAL_IO);

	if (r->wal_mode == WAL_NONE)
		return 0;

	struct wal_writer *writer = r->writer;

	struct wal_write_request *req = (struct wal_write_request *)
		palloc(fiber_ptr->gc_pool, sizeof(struct wal_write_request) +
		       sizeof(op) + row_len);

	req->fiber = fiber_ptr;
	req->res = -1;
	row_v11_fill(&req->row, lsn, XLOG, cookie, (const char *) &op,
		     sizeof(op), row, row_len);

	(void) tt_pthread_mutex_lock(&writer->mutex);

	bool input_was_empty = STAILQ_EMPTY(&writer->input);
	STAILQ_INSERT_TAIL(&writer->input, req, wal_fifo_entry);

	if (input_was_empty)
		(void) tt_pthread_cond_signal(&writer->cond);

	(void) tt_pthread_mutex_unlock(&writer->mutex);

	fiber_yield(); /* Request was inserted. */

	return req->res;
}

/* }}} */

/* {{{ SAVE SNAPSHOT and tarantool_box --cat */

void
snapshot_write_row(struct log_io *l,
		   const char *metadata, size_t metadata_len,
		   const char *data, size_t data_len)
{
	static uint64_t bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;

	struct row_v11 *row = (struct row_v11 *) palloc(fiber_ptr->gc_pool,
				     sizeof(struct row_v11) +
				     data_len + metadata_len);

	row_v11_fill(row, 0, SNAP, snapshot_cookie,
		     metadata, metadata_len, data, data_len);
	header_v11_sign(&row->header);


	size_t written = fwrite(row, 1, row_v11_size(row), l->f);

	if (written != row_v11_size(row)) {
		say_error("Can't write row (%zu bytes)", row_v11_size(row));
		panic_syserror("snapshot_write_row");
	}

	bytes += written;


	prelease_after(fiber_ptr->gc_pool, 128 * 1024);

	if (recovery_state->snap_io_rate_limit != UINT64_MAX) {
		if (last == 0) {
			/*
			 * Remember the time of first
			 * write to disk.
			 */
			ev_now_update();
			last = ev_now();
		}
		/**
		 * If io rate limit is set, flush the
		 * filesystem cache, otherwise the limit is
		 * not really enforced.
		 */
		if (bytes > recovery_state->snap_io_rate_limit)
			fdatasync(fileno(l->f));
	}
	while (bytes > recovery_state->snap_io_rate_limit) {
		ev_now_update();
		/*
		 * How much time have passed since
		 * last write?
		 */
		elapsed = ev_now() - last;
		/*
		 * If last write was in less than
		 * a second, sleep until the
		 * second is reached.
		 */
		if (elapsed < 1)
			usleep(((1 - elapsed) * 1000000));

		ev_now_update();
		last = ev_now();
		bytes -= recovery_state->snap_io_rate_limit;
	}
}

void
snapshot_save(struct recovery_state *r, void (*f) (struct log_io *))
{
	struct log_io *snap;
	snap = log_io_open_for_write(r->snap_dir, r->confirmed_lsn,
				     INPROGRESS);
	if (snap == NULL)
		panic_status(errno, "Failed to save snapshot: failed to open file in write mode.");
	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */
	say_info("saving snapshot `%s'",
		 format_filename(r->snap_dir, r->confirmed_lsn,
				 NONE));
	if (f)
		f(snap);

	log_io_close(&snap);

	say_info("done");
}

/**
 * Read WAL/SNAPSHOT and invoke a callback on every record (used
 * for --cat command line option).
 * @retval 0  success
 * @retval -1 error
 */

int
read_log(const char *filename,
	 row_handler *xlog_handler, row_handler *snap_handler,
	 void *param)
{
	struct log_dir *dir;
	row_handler *h;

	if (strstr(filename, wal_dir.filename_ext)) {
		dir = &wal_dir;
		h = xlog_handler;
	} else if (strstr(filename, snap_dir.filename_ext)) {
		dir = &snap_dir;
		h = snap_handler;
	} else {
		say_error("don't know how to read `%s'", filename);
		return -1;
	}

	FILE *f = fopen(filename, "r");
	struct log_io *l = log_io_open(dir, LOG_READ, filename, NONE, f);
	if (l == NULL)
		return -1;

	struct log_io_cursor i;

	log_io_cursor_open(&i, l);
	const char *row;
	uint32_t rowlen;
	while ((row = log_io_cursor_next(&i, &rowlen)))
		h(param, row, rowlen);

	log_io_cursor_close(&i);
	log_io_close(&l);
	return 0;
}

/* }}} */

