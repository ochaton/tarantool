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

usearch_index_t tt_usearch_init(usearch_init_options_t *options, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
	usearch_index_t index;
	options->quantization = usearch_scalar_f64_k;
	(void) options;
	usearch_init_options_t *opts = new usearch_init_options_t{};
	opts->metric_kind = usearch_metric_cos_k;
	opts->quantization = usearch_scalar_f64_k;
	opts->dimensions = 2;
	opts->expansion_add = 0; // for defaults
	opts->expansion_search = 0; // for defaults

	try {
		index = usearch_init(opts, uerror);
	} catch (std::exception &e) {
		*uerror = "usearch_init failed";
		return NULL;
	}

	try {
		usearch_reserve(index, 1024, uerror);
	} catch(std::exception &e) {
		usearch_free(index, uerror);
		*uerror = "usearch_reserve failed";
		return NULL;
	}

	usearch_key_t key = 1;
	double vector[2] = {0.1, 0.2};
	usearch_add(index, key, &vector[0], usearch_scalar_f64_k, uerror);
	if (!uerror) {
		std::cerr << "userach_add (in init) failed: " << *uerror << std::endl;
		return NULL;
	}
	std::cerr << "usearch_add completed: " << index << "\n";

	return index;
}

void tt_usearch_free(usearch_index_t index, usearch_error_t* uerror)
{
	std::cerr << __func__ << std::endl;
	try {
		usearch_free(index, uerror);
	} catch (std::exception &e) {
		*uerror = e.what();
	}
}

size_t tt_usearch_size(usearch_index_t index, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
	size_t size;
	try {

		size = usearch_size(index, uerror);
	} catch (std::exception &e) {
		*uerror = e.what();
	}
	return size;
}

size_t tt_usearch_memory_usage(usearch_index_t index, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
	size_t size;
	try {

		size = usearch_memory_usage(index, uerror);
	} catch (std::exception &e) {
		*uerror = e.what();
	}
	return size;
}

size_t tt_usearch_search(usearch_index_t index, tt_usearch_vector_t query, size_t limit, usearch_key_t *keys, usearch_distance_t *dists, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
	size_t matches;
	try {
		matches = usearch_search(index, query, usearch_scalar_f64_k, limit, keys, dists, uerror);
	} catch(std::exception &e) {
		std::cerr << "usearch_search: " << e.what() << std::endl;
		*uerror = e.what();
	}
	return matches;
}

void tt_usearch_add(usearch_index_t index, usearch_key_t key, tt_usearch_vector_t vector, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
//	try {
//		usearch_reserve(index, 1024, uerror);
//	} catch(std::exception &e) {
//		std::cerr << "usearch_add (reserve): " << e.what() << std::endl;
//		return;
//	}
	try {
		std::cerr << "usearch_add starting: " << index << "\n";
		usearch_add(index, key, &vector[0], usearch_scalar_f64_k, uerror);
	} catch(std::exception &e) {
		std::cerr << "usearch_add: " << e.what() << std::endl;
		*uerror = e.what();
	}
}

void tt_usearch_remove(usearch_index_t index, usearch_key_t key, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
	try {
		usearch_remove(index, key, uerror);
	} catch(std::exception &e) {
		std::cerr << "usearch_remove: " << e.what() << std::endl;
		*uerror = e.what();
	}
}

void tt_usearch_reserve(usearch_index_t index, size_t capacity, usearch_error_t *uerror)
{
	std::cerr << __func__ << std::endl;
	try {
		usearch_reserve(index, capacity, uerror);
	} catch(const std::exception& e) {
		std::cerr << "usearch_reserve: " << e.what() << std::endl;
		*uerror = e.what();
	}
}
