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


#ifndef __STATS_H__
#define __STATS_H__

#if defined(__cplusplus)
extern "C" {
#endif

#define STAT_DEF(name) struct stat *name ## _stat;

struct stat {
    ErlNifMutex *mutex;
    duration_t d;
    uint32_t h, n, num_samples;
    uint64_t min, max;
    double mean;
    uint64_t histogram[64];
    uint64_t samples[];
};

extern double __stat_mean(struct stat *s);
extern double __stat_mean_log2(struct stat *s);
extern uint64_t __stat_tick(struct stat *s);
extern void __stat_add(struct stat *s, uint64_t d);
extern void __stat_reset(struct stat *s);
extern void __stat_tock(struct stat *s);
extern void __stat_print_histogram(struct stat *s, const char *mod);
extern void __stat_free(struct stat *s);
extern struct stat *__stat_init(uint32_t n);

#if defined(__cplusplus)
}
#endif

#endif // __STATS_H__
