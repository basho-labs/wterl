/*
 * Copyright (C) 2013, all rights reserved by Gregory Burd <greg@burd.me>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * version 2 (MPLv2).  If a copy of the MPL was not distributed with this file,
 * you can obtain one at: http://mozilla.org/MPL/2.0/
 *
 * NOTES:
 *    - on some platforms this will require -lrt
 */
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <sys/timeb.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif


void current_utc_time(struct timespec *ts)
{
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    ts->tv_sec = mts.tv_sec;
    ts->tv_nsec = mts.tv_nsec;
#else
    clock_gettime(CLOCK_REALTIME, ts);
#endif

}

typedef enum { ns = 0, mcs, ms, s } time_scale;
struct scale_time {
     const char *abbreviation;
     const char *name;
     uint64_t mul, div, overhead, ticks_per;
};
static const struct scale_time scale[] = {
     { "ns",  "nanosecond",  1000000000LL, 1LL, 10, 2300000000000LL },
     { "mcs", "microsecond", 1000000LL, 1000LL, 10, 2300000000LL },
     { "ms",  "millisecond", 1000LL, 1000000LL, 10, 2300000LL },
     { "sec", "second",      1LL, 1000000000LL, 10, 2300LL } };

static uint64_t ts(time_scale unit)
{
    struct timespec ts;
    current_utc_time(&ts);
    return (((uint64_t)ts.tv_sec * scale[unit].mul) +
	    ((uint64_t)ts.tv_nsec / scale[unit].div));
}

#if defined(__i386__) || defined(__x86_64__)

/**
 * cpu_clock_ticks()
 *
 * A measure provided by Intel x86 CPUs which provides the number of cycles
 * (aka "ticks") executed as a counter using the RDTSC instruction.
 */
static inline uint64_t cpu_clock_ticks()
{
     uint32_t lo, hi;
     __asm__ __volatile__ (
	 "XORL %%eax, %%eax\n" /* Flush the pipeline */
	 "CPUID\n"
	 "RDTSC\n"             /* Get RDTSC counter in edx:eax */
	 : "=a" (lo), "=d" (hi)
	 :
	 : "%ebx", "%ecx" );
     return (uint64_t)hi << 32 | lo;
}

#endif

#if 0

/**
 * cpu_clock_ticks()
 *
 * An approximation of elapsed [ns, mcs, ms, s] from CPU clock ticks.
 */
static uint64_t elapsed_cpu_clock_ticks(uint64_t start, time_scale unit)
{
    return (cpu_clock_ticks() - start - scale[unit].overhead) * scale[unit].ticks_per;
}

#endif

typedef struct {
     uint64_t then;
     time_scale unit;
} duration_t;

static inline uint64_t elapsed(duration_t *d)
{
     uint64_t now = ts(d->unit);
     uint64_t elapsed = now - d->then;
     d->then = now;
     return elapsed;
}

#define DURATION(name, resolution) duration_t name =    \
     {ts(resolution), resolution}

#define ELAPSED_DURING(result, resolution, block)       \
     do {                                               \
	  DURATION(__x, resolution);                    \
	  do block while(0);                            \
	  *result = elapsed(&__x);                      \
     } while(0);

#define CYCLES_DURING(result, block)                    \
     do {                                               \
	 uint64_t __begin = cpu_clock_ticks();          \
	 do block while(0);                             \
	 *result = cpu_clock_ticks() - __begin;         \
     } while(0);
