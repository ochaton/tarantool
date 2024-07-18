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

#pragma once

#include <cstddef>
#include "usearch/c/usearch.h"

struct tt_usearch_index {
	size_t reserved;
	usearch_scalar_kind_t quantization;
	usearch_index_t usearch;
};

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

typedef void *tt_usearch_vector_t;

typedef float* tt_usearch_vector_f32_t;
typedef double* tt_usearch_vector_f64_t;
typedef int8_t* tt_usearch_vector_i8_t;

extern tt_usearch_index *tt_usearch_init(usearch_init_options_t* options, usearch_error_t *uerror);
extern void tt_usearch_free(tt_usearch_index *index, usearch_error_t* uerror);

extern size_t tt_usearch_size(tt_usearch_index *index, usearch_error_t* uerror);
extern size_t tt_usearch_memory_usage(tt_usearch_index *index, usearch_error_t* uerror);

extern size_t tt_usearch_search(tt_usearch_index *index, tt_usearch_vector_t query, size_t limit, usearch_key_t *keys, usearch_distance_t* dists, usearch_error_t* uerror);

extern void tt_usearch_add(tt_usearch_index *index, usearch_key_t key, tt_usearch_vector_t vector, usearch_error_t* uerror);
extern void tt_usearch_remove(tt_usearch_index *index, usearch_key_t key, usearch_error_t* uerror);

extern void tt_usearch_reserve(tt_usearch_index *index, size_t capacity, usearch_error_t* uerror);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
