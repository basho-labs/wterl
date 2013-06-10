/*
 * wterl: an Erlang NIF for WiredTiger
 *
 * Copyright (c) 2012-2013 Basho Technologies, Inc. All Rights Reserved.
 *
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

/*
 * Most of the following source code is copied directly from: "The Lock-Free
 * Library" (http://www.cl.cam.ac.uk/research/srg/netos/lock-free/) reused and
 * redistrubuted in accordance with their license:
 *
 * Copyright (c) 2002-2003 K A Fraser, All Rights Reserved.
 *
 * * Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are
 *   met:
 *
 * * Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * The name of the author may not be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __CAS_H_
#define __CAS_H_

#define CACHE_LINE_SIZE 64

#define ATOMIC_ADD_TO(_v,_x)                                            \
do {                                                                    \
    int __val = (_v), __newval;                                         \
    while ( (__newval = CASIO(&(_v),__val,__val+(_x))) != __val )       \
        __val = __newval;                                               \
} while ( 0 )

#define ATOMIC_SET_TO(_v,_x)                                            \
do {                                                                    \
    int __val = (_v), __newval;                                         \
    while ( (__newval = CASIO(&(_v),__val,__val=(_x))) != __val )       \
        __val = __newval;                                               \
} while ( 0 )

#define ALIGNED_ENIF_ALLOC(_s)                                      \
    ((void *)(((unsigned long)enif_alloc((_s)+CACHE_LINE_SIZE*2) +  \
               CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE-1)))        \

/*
 * I. Compare-and-swap.
 */

/*
 * This is a strong barrier! Reads cannot be delayed beyond a later store.
 * Reads cannot be hoisted beyond a LOCK prefix. Stores always in-order.
 */
#define CAS(_a, _o, _n)                                    \
({ __typeof__(_o) __o = _o;                                \
   __asm__ __volatile__(                                   \
       "lock cmpxchg %3,%1"                                \
       : "=a" (__o), "=m" (*(volatile unsigned int *)(_a)) \
       :  "0" (__o), "r" (_n) );                           \
   __o;                                                    \
})

#define FAS(_a, _n)                                        \
({ __typeof__(_n) __o;                                     \
   __asm__ __volatile__(                                   \
       "lock xchg %0,%1"                                   \
       : "=r" (__o), "=m" (*(volatile unsigned int *)(_a)) \
       :  "0" (_n) );                                      \
   __o;                                                    \
})

#define CAS64(_a, _o, _n)                                        \
({ __typeof__(_o) __o = _o;                                      \
   __asm__ __volatile__(                                         \
       "movl %3, %%ecx;"                                         \
       "movl %4, %%ebx;"                                         \
       "lock cmpxchg8b %1"                                       \
       : "=A" (__o), "=m" (*(volatile unsigned long long *)(_a)) \
       : "0" (__o), "m" (_n >> 32), "m" (_n)                     \
       : "ebx", "ecx" );                                         \
   __o;                                                          \
})

/* Update Integer location, return Old value. */
#define CASIO CAS
#define FASIO FAS
/* Update Pointer location, return Old value. */
#define CASPO CAS
#define FASPO FAS
/* Update 32/64-bit location, return Old value. */
#define CAS32O CAS
#define CAS64O CAS64

/*
 * II. Memory barriers.
 *  WMB(): All preceding write operations must commit before any later writes.
 *  RMB(): All preceding read operations must commit before any later reads.
 *  MB():  All preceding memory accesses must commit before any later accesses.
 *
 *  If the compiler does not observe these barriers (but any sane compiler
 *  will!), then VOLATILE should be defined as 'volatile'.
 */

#define MB()  __asm__ __volatile__ ("lock; addl $0,0(%%esp)" : : : "memory")
#define WMB() __asm__ __volatile__ ("" : : : "memory")
#define RMB() MB()
#define VOLATILE /*volatile*/

/* On Intel, CAS is a strong barrier, but not a compile barrier. */
#define RMB_NEAR_CAS() WMB()
#define WMB_NEAR_CAS() WMB()
#define MB_NEAR_CAS()  WMB()


/*
 * III. Cycle counter access.
 */

typedef unsigned long long tick_t;
#define RDTICK() \
    ({ tick_t __t; __asm__ __volatile__ ("rdtsc" : "=A" (__t)); __t; })

#endif /* __CAS_H_ */
