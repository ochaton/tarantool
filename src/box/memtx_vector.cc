/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
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
#include "memtx_vector.h"

#include <small/small.h>
#include <small/mempool.h>

#include "index.h"
#include "errinj.h"
#include "fiber.h"
#include "trivia/util.h"

#include "tuple.h"
#include "txn.h"
#include "memtx_tx.h"
#include "space.h"
#include "schema.h"
#include "memtx_engine.h"

#include "usearch/c/usearch.h"

struct memtx_vector_index {
	struct index base;
	unsigned dimension;
	usearch_index_t tree;
};

typedef double* tt_usearch_vector_t;

struct vector_iterator {
	/* Dimension of the Index */
	uint32_t dim;
	/* Resulting set */
	size_t result_count;

	/* Current position of the iterator */
	size_t position;

	/* Initial Query Vector of dim size */
	tt_usearch_vector_t query;
	/* Result keys (result_count size) */
	usearch_key_t *keys;
	/* Result distances (result_count size) */
	usearch_distance_t *dists;

	uint32_t space_id;
};

struct index_vector_iterator {
        struct iterator base;
        struct vector_iterator impl;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

static inline int
mp_decode_num(const char **data, uint32_t fieldno, double *ret)
{
	if (mp_read_double(data, ret) != 0) {
		diag_set(ClientError, ER_FIELD_TYPE,
			 int2str(fieldno + TUPLE_INDEX_BASE),
			 field_type_strs[FIELD_TYPE_NUMBER],
			 mp_type_strs[mp_typeof(**data)]);
		return -1;
	}
	return 0;
}

/**
 * Extract coordinates of rectangle from message packed string.
 * There must be <count> or <count * 2> numbers in that string.
 */
static inline int
mp_decode_vector(tt_usearch_vector_t vec, unsigned dimension,
	       const char *mp, unsigned count, const char *what)
{
	double c = 0;
	if (count == dimension) { /* point */
		for (unsigned i = 0; i < dimension; i++) {
			if (mp_decode_num(&mp, i, &c) < 0)
				return -1;
			vec[i] = c;
		}
	} else {
		diag_set(ClientError, ER_RTREE_RECT,
			 what, dimension, dimension * 2);
		return -1;
	}
	return 0;
}

static inline int
extract_vector(tt_usearch_vector_t vec, struct tuple *tuple,
		  struct index_def *index_def)
{
	assert(index_def->key_def->part_count == 1);
	assert(!index_def->key_def->is_multikey);
	const char *elems = tuple_field_by_part(tuple,
				index_def->key_def->parts, MULTIKEY_NONE);
	unsigned dimension = index_def->opts.dimension;
	uint32_t count = mp_decode_array(&elems);
	return mp_decode_vector(vec, dimension, elems, count, "Field");
}

static inline int
extract_key(usearch_key_t *key, struct tuple *tuple, struct index_def *index_def) {
	const char *pk = tuple_field_by_part(tuple, index_def->pk_def->parts, MULTIKEY_NONE);
	*key = mp_decode_int(&pk);
	return 0;
}


/* {{{ MemtxVector  **********************************************************/

static void
memtx_vector_index_destroy(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_error_t uerror = NULL;
	usearch_free(&index->tree, &uerror);
	free(index);
}

static bool
memtx_vector_index_def_change_requires_rebuild(struct index *index,
					      const struct index_def *new_def)
{
	if (memtx_index_def_change_requires_rebuild(index, new_def))
		return true;
	if (index->def->opts.distance != new_def->opts.distance ||
	    index->def->opts.dimension != new_def->opts.dimension)
		return true;
	return false;

}

static ssize_t
memtx_vector_index_size(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_error_t uerror = NULL;
	return usearch_size(index->tree, &uerror);
}

static ssize_t
memtx_vector_index_bsize(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_error_t uerror = NULL;
	return usearch_memory_usage(index->tree, &uerror);
}

static ssize_t
memtx_vector_index_count(struct index *base, enum iterator_type type,
			const char *key, uint32_t part_count)
{
	if (type == ITER_ALL)
		return memtx_vector_index_size(base); /* optimization */
	return 0;
}

static int
memtx_vector_index_get_internal(struct index *base, const char *key,
			       uint32_t part_count, struct tuple **result)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	*result = NULL;

	int64_t dim = index->base.def->opts.dimension;
	tt_usearch_vector_t query = (tt_usearch_vector_t) xcalloc(dim, sizeof(tt_usearch_vector_t));
	if (mp_decode_vector(query, dim, key, part_count, "Key") != 0)
		return -1;

	usearch_error_t uerror = NULL;
	usearch_key_t keys[1];
	usearch_distance_t dists[1];
	size_t matches = usearch_search(&index->tree, query, usearch_scalar_f64_k, 1, keys, dists, &uerror);
	if (!matches || dists[0] != 0)
		return 0;

	struct txn *txn = in_txn();
	struct space *space = space_by_id(base->def->space_id);

	char pk[10];
	mp_encode_int(pk, keys[0]);

	return index_get_internal(space->index[0], pk, 1, result);
}

static int
memtx_vector_index_replace(struct index *base, struct tuple *old_tuple,
			  struct tuple *new_tuple, enum dup_replace_mode mode,
			  struct tuple **result, struct tuple **successor)
{
	(void)mode;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;

	/* RTREE index doesn't support ordering. */
	*successor = NULL;

	usearch_error_t uerror = NULL;

	int64_t dim = index->base.def->opts.dimension;
	tt_usearch_vector_t vec = (tt_usearch_vector_t) xcalloc(dim, sizeof(tt_usearch_vector_t));
	usearch_key_t key;

	if (new_tuple) {
		if (extract_vector(vec, new_tuple, base->def) != 0)
			return -1;
		if (extract_key(&key, new_tuple, base->def) != 0)
			return -1;

		try {
			usearch_add(&index->tree, key, vec, usearch_scalar_f64_k, &uerror);
		} catch (std::exception& e) {

		}
		if (uerror != NULL)
			return -1;
	}
	if (old_tuple) {
		if (extract_vector(vec, old_tuple, base->def) != 0)
			return -1;
		if (extract_key(&key, old_tuple, base->def) != 0)
			return -1;

		usearch_remove(&index->tree, key, &uerror);
		if (uerror != NULL)
			old_tuple = NULL;
	}
	*result = old_tuple;
	return 0;
}

static int
memtx_vector_index_reserve(struct index *base, uint32_t size_hint)
{
	(void)size_hint;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;

	usearch_error_t uerror = NULL;
	usearch_reserve(index->tree, size_hint, &uerror);

	if (uerror != NULL) {
		/* same error as in mempool_alloc */
		diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
			 "usearch", tt_sprintf("%s", uerror));
		return -1;
	}
	return 0;
}


/**************************** Vector Iterator ****************************/

static void
vector_iterator_init(struct vector_iterator *itr)
{
	assert(itr->result_count > 0);
	itr->keys = (usearch_key_t *) xcalloc(itr->result_count, sizeof(usearch_key_t));
	itr->dists = (usearch_distance_t *) xcalloc(itr->result_count, sizeof(usearch_distance_t));
}

static void
vector_iterator_destroy(struct vector_iterator *itr)
{
	free(itr->keys);
	free(itr->dists);
	free(itr->query);
	itr->result_count = 0;
}

static struct tuple *vector_iterator_next(struct vector_iterator *itr, struct tuple **result)
{
	*result = NULL;
	if (itr->position < itr->result_count) {
		usearch_key_t key = itr->keys[itr->position];
		usearch_distance_t dist = itr->dists[itr->position];

		char pk[10];
		mp_encode_int(pk, key);

		struct space *space = space_by_id(itr->space_id);
		++itr->position;

		index_get_internal(space->index[0], pk, 1, result);
	}
	return *result;
}

static int
index_vector_iterator_next(struct iterator *i, struct tuple **ret)
{
	struct index_vector_iterator *itr = (struct index_vector_iterator *)i;
	struct space *space;
	struct index *index;
	index_weak_ref_get_checked(&i->index_ref, &space, &index);
	do {
		*ret = (struct tuple *) vector_iterator_next(&itr->impl, ret);
		if (*ret == NULL)
			break;
		struct txn *txn = in_txn();
		*ret = memtx_tx_tuple_clarify(txn, space, *ret, index, 0);
/********MVCC TRANSACTION MANAGER STORY GARBAGE COLLECTION BOUND START*********/
		memtx_tx_story_gc();
/*********MVCC TRANSACTION MANAGER STORY GARBAGE COLLECTION BOUND END**********/
	} while (*ret == NULL);
	return 0;
}


static void
index_vector_iterator_free(struct iterator *i)
{
	struct index_vector_iterator *itr = (struct index_vector_iterator *)i;
	vector_iterator_destroy(&itr->impl);
	mempool_free(itr->pool, itr);
}

static void vector_search(usearch_index_t *index, const tt_usearch_vector_t query, struct vector_iterator *itr)
{
	usearch_error_t uerror = NULL;
	size_t n_matches = usearch_search(index, itr->query, usearch_scalar_f64_k, itr->result_count, itr->keys, itr->dists, &uerror);
	itr->result_count = n_matches;
}

static struct iterator *
memtx_vector_index_create_iterator(struct index *base, enum iterator_type type,
				 const char *key, uint32_t part_count,
				 const char *pos)
{
	if (type != ITER_NEIGHBOR) {
		diag_set(IllegalParams, "usearch index supports only NEIGHBOUR type");
		return NULL;
	}

	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;
	struct index_vector_iterator *it = (struct index_vector_iterator *)
		mempool_alloc(&memtx->iterator_pool);

	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct index_vector_iterator),
			 "memtx_vector_index", "iterator");
		return NULL;
	}

	int64_t dim = index->base.def->opts.dimension;
	tt_usearch_vector_t query = (tt_usearch_vector_t) xcalloc(dim, sizeof(tt_usearch_vector_t));

	if (mp_decode_vector(query, dim, key, part_count, "Key") != 0)
		return NULL;

	iterator_create(&it->base, base);
	it->pool = &memtx->iterator_pool;
	it->base.next_internal = index_vector_iterator_next;
	it->base.next = memtx_iterator_next;
	it->base.position = generic_iterator_position;
	it->base.free = index_vector_iterator_free;

	it->impl.query = query;
	it->impl.dim = dim;
	it->impl.result_count = index->base.def->opts.expansion_search;
	vector_iterator_init(&it->impl);

	it->impl.space_id = base->def->space_id;

	vector_search(&index->tree, query, &it->impl);
	return (struct iterator *)it;
}

/**************************** End of Vector Iterator ****************************/


static const struct index_vtab memtx_vector_index_vtab = {
	/* .destroy = */ memtx_vector_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ generic_index_update_def,
	/* .depends_on_pk = */ generic_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		memtx_vector_index_def_change_requires_rebuild,
	/* .size = */ memtx_vector_index_size,
	/* .bsize = */ memtx_vector_index_bsize,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ generic_index_random,
	/* .count = */ memtx_vector_index_count,
	/* .get_internal = */ memtx_vector_index_get_internal,
	/* .get = */ memtx_index_get,
	/* .replace = */ memtx_vector_index_replace,
	/* .create_iterator = */ memtx_vector_index_create_iterator,
	/* .create_read_view = */ generic_index_create_read_view,
	/* .stat = */ generic_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
	/* .begin_build = */ generic_index_begin_build,
	/* .reserve = */ memtx_vector_index_reserve,
	/* .build_next = */ generic_index_build_next,
	/* .end_build = */ generic_index_end_build,
};

struct index *
memtx_vector_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	assert(def->iid > 0);
	assert(def->key_def->part_count == 1);
	assert(def->key_def->parts[0].type == FIELD_TYPE_ARRAY);
	assert(def->opts.is_unique == false);

	if (def->opts.dimension < 1 ||
	    def->opts.dimension > VECTOR_MAX_DIMENSION) {
		diag_set(UnsupportedIndexFeature, def,
			 tt_sprintf("dimension (%lld): must belong to "
				    "range [%u, %u]",
				    (long long)def->opts.dimension, 1,
				    VECTOR_MAX_DIMENSION));
		return NULL;
	}

	struct memtx_vector_index *index =
		(struct memtx_vector_index *)xcalloc(1, sizeof(*index));
	index_create(&index->base, (struct engine *)memtx,
		     &memtx_vector_index_vtab, def);

	index->dimension = def->opts.dimension;

	usearch_init_options_t uopts = {
		.dimensions = (size_t) def->opts.dimension,
		.connectivity = def->opts.connectivity,
		.expansion_add = def->opts.expansion_add,
		.expansion_search = def->opts.expansion_search,
		.metric_kind = (usearch_metric_kind_t) def->opts.vector_metric_kind,
		.metric = NULL,
		.quantization = usearch_scalar_f64_k,
	};

	fprintf(stderr, "DEBUG: dim=%d con=%d ea=%d es=%d mk=%d", uopts.dimensions,
		uopts.connectivity, uopts.expansion_add, uopts.expansion_search, uopts.metric_kind);

	usearch_error_t uerror = NULL;
	index->tree = usearch_init(&uopts, &uerror);
	if (uerror != NULL) {
		diag_set(UnsupportedIndexFeature, def,
			 tt_sprintf("usearch failed: %s", uerror));
		return NULL;
	}
	return &index->base;
}
