#ifndef INCLUDES_TARANTOOL_BOX_VY_MEM_H
#define INCLUDES_TARANTOOL_BOX_VY_MEM_H
/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
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
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <stdint.h>
#include <stdbool.h>

#include <small/rlist.h>

#include "vy_stmt.h" /* for comparators */

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct vy_mem;
struct vy_stmt;
struct lsregion;

/** @cond false */

struct tree_mem_key {
	const struct tuple *stmt;
	int64_t lsn;
};

/**
 * Internal. Extracted to speed up BPS tree.
 */
static int
vy_mem_tree_cmp(const struct tuple *a, const struct tuple *b,
		const struct key_def *key_def)
{
	int res = vy_stmt_compare(a, b, key_def);
	if (res)
		return res;
	int64_t a_lsn = vy_stmt_lsn(a), b_lsn = vy_stmt_lsn(b);
	return a_lsn > b_lsn ? -1 : a_lsn < b_lsn;
}

/**
 * Internal. Extracted to speed up BPS tree.
 */
static int
vy_mem_tree_cmp_key(const struct tuple *a, struct tree_mem_key *key,
		    const struct key_def *key_def)
{
	int res = vy_stmt_compare(a, key->stmt, key_def);
	if (res == 0) {
		if (key->lsn == INT64_MAX - 1)
			return 0;
		int64_t a_lsn = vy_stmt_lsn(a);
		res = a_lsn > key->lsn ? -1 : a_lsn < key->lsn;
	}
	return res;
}

#define VY_MEM_TREE_EXTENT_SIZE (16 * 1024)

#define BPS_TREE_NAME vy_mem_tree
#define BPS_TREE_BLOCK_SIZE 512
#define BPS_TREE_EXTENT_SIZE VY_MEM_TREE_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, key_def) vy_mem_tree_cmp(a, b, key_def)
#define BPS_TREE_COMPARE_KEY(a, b, key_def) vy_mem_tree_cmp_key(a, b, key_def)
#define bps_tree_elem_t const struct tuple *
#define bps_tree_key_t struct tree_mem_key *
#define bps_tree_arg_t const struct key_def *
#define BPS_TREE_NO_DEBUG

#include <salad/bps_tree.h>

/** @endcond false */

/**
 * vy_mem is an in-memory container for tuples in a single vinyl
 * range.
 * Internally it uses bps_tree to store tuples, which are ordered
 * by statement key and, for the same key, by lsn, in descending
 * order.
 *
 * For example, assume there are two statements with the same key,
 * but different LSN. These are duplicates of the same key,
 * maintained for the purpose of MVCC/consistent read view.
 * In Vinyl terms, they form a duplicate chain.
 *
 * vy_mem distinguishes between the first duplicate in the chain
 * and other keys in that chain.
 */
struct vy_mem {
	/** Link in range->frozen list. */
	struct rlist in_frozen;
	/** Link in scheduler->dirty_mems list. */
	struct rlist in_dirty;
	/** BPS tree */
	struct vy_mem_tree tree;
	/** The total size of all tuples in this tree in bytes */
	size_t used;
	/** The minimum value of stmt->lsn in this tree */
	int64_t min_lsn;
	/* A key definition for this index. */
	struct key_def *key_def;
	/** version is initially 0 and is incremented on every write */
	uint32_t version;
	/** Allocator for extents */
	struct lsregion *allocator;
	/** The last LSN for lsregion allocator */
	const int64_t *allocator_lsn;
};

/**
 * Instantiate a new in-memory level.
 *
 * @param key_def key definition.
 * @param format tuple format.
 * @param allocator lsregioni allocator to use for BPS tree extents
 * @param allocator_lsn a pointer to the latest LSN for lsregion.
 * @retval new vy_mem instance on success.
 * @retval NULL on error, check diag.
 */
struct vy_mem *
vy_mem_new(struct key_def *key_def, struct lsregion *allocator,
	   const int64_t *allocator_lsn);

/**
 * Delete in-memory level.
 */
void
vy_mem_delete(struct vy_mem *index);

/*
 * Return the older statement for the given one.
 */
const struct tuple *
vy_mem_older_lsn(struct vy_mem *mem, const struct tuple *stmt);

/**
 * Insert a statement into the in-memory level.
 *
 * @param mem vy_mem
 * @param stmt statement
 * @param alloc_lsn LSN for lsregion allocator
 * @retval 0 on success
 * @retval -1 on error, check diag
 */
int
vy_mem_insert(struct vy_mem *mem, const struct tuple *stmt,
	      int64_t alloc_lsn);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* INCLUDES_TARANTOOL_BOX_VY_MEM_H */
