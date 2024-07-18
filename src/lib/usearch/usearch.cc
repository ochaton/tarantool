/*
 * Copyright 2010-2024, Tarantool AUTHORS, please see AUTHORS file.
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

#include "usearch.h"
#include <exception>
#include <iostream>
#include "say.h"

enum {
	TT_USEARCH_MAX_RESERVE = 8192,
};

static inline
uint64_t next_pow2(uint64_t x) {
	return x == 1 ? 1 : 1<<(64-__builtin_clzl(x-1));
}

static inline
size_t next_reserve(size_t size)
{
	size_t s2 = next_pow2(size);
	size_t r = 2*s2;
	return r > TT_USEARCH_MAX_RESERVE ? s2+TT_USEARCH_MAX_RESERVE : r;
}

tt_usearch_index *tt_usearch_init(usearch_init_options_t *options, usearch_error_t *uerror)
{
	usearch_index_t index;
	try {
		index = usearch_init(options, uerror);
	} catch (std::exception &e) {
		*uerror = "usearch_init failed";
		return NULL;
	}

	tt_usearch_index *ret = new tt_usearch_index{};
	ret->quantization = options->quantization;
	ret->usearch = index;
	ret->reserved = 0;

	return ret;
}

void tt_usearch_free(tt_usearch_index *index, usearch_error_t* uerror)
{
	try {
		usearch_free(index->usearch, uerror);
		delete index;
	} catch (std::exception &e) {
		*uerror = e.what();
	}
}

size_t tt_usearch_size(tt_usearch_index *index, usearch_error_t *uerror)
{
	size_t size;
	try {

		size = usearch_size(index->usearch, uerror);
	} catch (std::exception &e) {
		*uerror = e.what();
	}
	return size;
}

size_t tt_usearch_memory_usage(tt_usearch_index *index, usearch_error_t *uerror)
{
	size_t size;
	try {

		size = usearch_memory_usage(index->usearch, uerror);
	} catch (std::exception &e) {
		*uerror = e.what();
	}
	return size;
}

size_t tt_usearch_search(tt_usearch_index *index, tt_usearch_vector_t query, size_t limit, usearch_key_t *keys, usearch_distance_t *dists, usearch_error_t *uerror)
{
	size_t matches;
	try {
		matches = us_usearch_search(index->usearch, query, index->quantization, limit, keys, dists, uerror);
	} catch(std::exception &e) {
		*uerror = e.what();
	}
	return matches;
}

void tt_usearch_add(tt_usearch_index *index, usearch_key_t key, tt_usearch_vector_t vector, usearch_error_t *uerror)
{
	size_t size = usearch_size(index->usearch, uerror);
	if (index->reserved <= size) {
		size_t new_reserve = next_reserve(size);
		// say_debug("tt_usearch_add(reserved=%lu, size=%lu, next_reserve=%lu)", index->reserved, size, new_reserve);
		try {

			usearch_reserve(index->usearch, new_reserve, uerror);
		} catch(std::exception &e) {
			*uerror = e.what();
			return;
		}
		index->reserved = new_reserve;
	}
	try {
		usearch_add(index->usearch, key, vector, index->quantization, uerror);
	} catch(std::exception &e) {
		*uerror = e.what();
	}
}

void tt_usearch_remove(tt_usearch_index *index, usearch_key_t key, usearch_error_t *uerror)
{
	try {
		usearch_remove(index->usearch, key, uerror);
	} catch(std::exception &e) {
		*uerror = e.what();
	}
}

void tt_usearch_reserve(tt_usearch_index *index, size_t capacity, usearch_error_t *uerror)
{
	if (capacity <= index->reserved) {
		return;
	}
	// say_debug("tt_usearch_reserve(reserved=%lu, capacity=%lu)", index->reserved, capacity);
	try {
		usearch_reserve(index->usearch, capacity, uerror);
		index->reserved = capacity;
	} catch(const std::exception& e) {
		*uerror = e.what();
	}
}
