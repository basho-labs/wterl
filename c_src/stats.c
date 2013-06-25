/*
 * stats:
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 * Author: Gregory Burd <greg@basho.com> <greg@burd.me>
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <inttypes.h>

#include "erl_nif.h"
#include "erl_driver.h"

#include "common.h"
#include "duration.h"
#include "stats.h"

/**
 * Calculate the log2 of 64bit unsigned integers.
 */
#ifdef __GCC__
#define LOG2(X) ((unsigned) ((8 * (sizeof(uint64_t) - 1))  - __builtin_clzll((X))))
#else
static unsigned int __log2_64(uint64_t x) {
     static const int tab64[64] = {
          63,  0, 58,  1, 59, 47, 53,  2,
          60, 39, 48, 27, 54, 33, 42,  3,
          61, 51, 37, 40, 49, 18, 28, 20,
          55, 30, 34, 11, 43, 14, 22,  4,
          62, 57, 46, 52, 38, 26, 32, 41,
          50, 36, 17, 19, 29, 10, 13, 21,
          56, 45, 25, 31, 35, 16,  9, 12,
          44, 24, 15,  8, 23,  7,  6,  5};
     if (x == 0) return 0;
     uint64_t v = x;
     v |= v >> 1;
     v |= v >> 2;
     v |= v >> 4;
     v |= v >> 8;
     v |= v >> 16;
     v |= v >> 32;
     return tab64[((uint64_t)((v - (v >> 1)) * 0x07EDD5E59A4E28C2)) >> 58];
}
#define LOG2(X) __log2_64(X)
#endif

double
__stat_mean(struct stat *s)
{
    uint32_t t, h;
    double mean;

    if (!s)
	return 0.0;

    t = s->h;
    h = (s->h + 1) % s->num_samples;
    mean = 0;

    while (h != t) {
	mean += s->samples[h];
	h = (h + 1) % s->num_samples;
    }
    if (mean > 0)
	mean /= (double)(s->n < s->num_samples ? s->n : s->num_samples);
    return mean;
}

double
__stat_mean_log2(struct stat *s)
{
    uint32_t i;
    double mean;

    if (!s)
	return 0.0;

    mean = 0;
    for (i = 0; i < 64; i++)
	mean += (s->histogram[i] * i);
    if (mean > 0)
	mean /= (double)s->n;
    return mean;
}

uint64_t
__stat_tick(struct stat *s)
{
    duration_t *d;
    uint64_t t;

    if (!s)
	return 0.0;

    d = (duration_t*)erl_drv_tsd_get(s->duration_key);
    if (!d) {
	if ((d = enif_alloc(sizeof(duration_t))) == NULL)
	    return 0;
	memset(d, 0, sizeof(duration_t));
	erl_drv_tsd_set(s->duration_key, d);
    }
    t = ts(d->unit);
    d->then = t;
    return t;
}

void
__stat_reset(struct stat *s)
{
    duration_t *d;

    if (!s)
	return;

    s->min = ~0;
    s->max = 0;
    s->h = 0;
    memset(s->histogram, 0, sizeof(uint64_t) * 64);
    memset(s->samples, 0, sizeof(uint64_t) * s->num_samples);
    d = (duration_t*)erl_drv_tsd_get(s->duration_key);
    if (d)
	d->then = 0;
}

uint64_t
__stat_tock(struct stat *s)
{
    uint64_t now;
    uint64_t elapsed;
    uint32_t i;
    duration_t *d;

    if (!s)
	return 0.0;

    d = (duration_t*)erl_drv_tsd_get(s->duration_key);
    if (!d)
	return 0;

    now = ts(d->unit);
    elapsed = now - d->then;
    i = s->h;
    if (s->n == s->num_samples) {
	s->mean = (s->mean + __stat_mean(s)) / 2.0;
	if (s->n >= 4294967295)
	    __stat_reset(s);
    }
    s->h = (s->h + 1) % s->num_samples;
    s->samples[i] = elapsed;
    if (elapsed < s->min)
	s->min = elapsed;
    if (elapsed > s->max)
	s->max = elapsed;
    s->histogram[LOG2(elapsed)]++;
    s->n++;
    d->then = ts(d->unit);
    return elapsed;
}

void
__stat_print_histogram(struct stat *s, const char *mod)
{
    uint8_t logs[64];
    uint8_t i, j, max_log = 0;
    double m;

    if (!s)
	return;

    m = (s->mean + __stat_mean(s) / 2.0);

    fprintf(stderr, "%s:async_nif request latency histogram:\n", mod);
    for (i = 0; i < 64; i++) {
	logs[i] = LOG2(s->histogram[i]);
	if (logs[i] > max_log)
	    max_log = logs[i];
    }
    for (i = max_log; i > 0; i--) {
	if (!(i % 10))
	    fprintf(stderr, "2^%2d ", i);
	else
	    fprintf(stderr, "     ");
	for(j = 0; j < 64; j++)
	    fprintf(stderr, logs[j] >= i ?  "•" : " ");
	fprintf(stderr, "\n");
    }
    if (max_log == 0) {
	fprintf(stderr, "[empty]\n");
    } else {
	fprintf(stderr, "     ns        μs        ms        s         ks\n");
	fprintf(stderr, "min: ");
	if (s->min < 1000)
	    fprintf(stderr, "%llu (ns)", PRIuint64(s->min));
	else if (s->min < 1000000)
	    fprintf(stderr, "%.2f (μs)", s->min / 1000.0);
	else if (s->min < 1000000000)
	    fprintf(stderr, "%.2f (ms)", s->min / 1000000.0);
	else if (s->min < 1000000000000)
	    fprintf(stderr, "%.2f (s)", s->min / 1000000000.0);
	fprintf(stderr, "  max: ");
	if (s->max < 1000)
	    fprintf(stderr, "%llu (ns)", PRIuint64(s->max));
	else if (s->max < 1000000)
	    fprintf(stderr, "%.2f (μs)", s->max / 1000.0);
	else if (s->max < 1000000000)
	    fprintf(stderr, "%.2f (ms)", s->max / 1000000.0);
	else if (s->max < 1000000000000)
	    fprintf(stderr, "%.2f (s)", s->max / 1000000000.0);
	fprintf(stderr, "  mean: ");
	if (m < 1000)
	    fprintf(stderr, "%.2f (ns)", m);
	else if (m < 1000000)
	    fprintf(stderr, "%.2f (μs)", m / 1000.0);
	else if (m < 1000000000)
	    fprintf(stderr, "%.2f (ms)", m / 1000000.0);
	else if (m < 1000000000000)
	    fprintf(stderr, "%.2f (s)", m / 1000000000.0);
	fprintf(stderr, "\n");
    }
    fflush(stderr);
}

void
__stat_free(struct stat *s)
{
    if (!s)
	return;

    enif_free(s->samples);
    enif_free(s);
}

struct stat *
__stat_init(uint32_t n)
{
    struct stat *s = enif_alloc(sizeof(struct stat) + (sizeof(uint64_t) * n));
    if (!s)
	return NULL;
    memset(s, 0, sizeof(struct stat) + (sizeof(uint64_t) * n));
    s->min = ~0;
    s->max = 0;
    s->mean = 0.0;
    s->h = 0;
    s->num_samples = n;
    erl_drv_tsd_key_create(NULL, &(s->duration_key));
    return s;
}
