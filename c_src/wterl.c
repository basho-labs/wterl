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

#include "erl_nif.h"
#include "erl_driver.h"

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <inttypes.h>
#include <errno.h>

#include "wiredtiger.h"

#include "common.h"
#include "duration.h"
#include "stats.h"
#include "async_nif.h"
#include "queue.h"
#include "cas.h"

#define MAX_CACHE_SIZE ASYNC_NIF_MAX_WORKERS

static ErlNifResourceType *wterl_conn_RESOURCE;
static ErlNifResourceType *wterl_cursor_RESOURCE;

typedef char Uri[128];

struct wterl_ctx {
    STAILQ_ENTRY(wterl_ctx) entries;
    uint64_t tstamp;
    uint64_t sig;
    size_t sig_len;
    WT_SESSION *session;
    uint32_t num_cursors;
    const char *session_config;
    struct cursor_info {
        const char *uri;
        const char *config;
        WT_CURSOR *cursor;
    } ci[]; // Note: must be last in struct
};

typedef struct wterl_conn {
    WT_CONNECTION *conn;
    const char *session_config;
    STAILQ_HEAD(ctxs, wterl_ctx) cache;
    ErlNifMutex *cache_mutex;
    uint32_t cache_size;
    struct wterl_ctx *mru_ctx[ASYNC_NIF_MAX_WORKERS];
    uint64_t histogram[64];
    uint64_t histogram_count;
} WterlConnHandle;

typedef struct {
    WT_SESSION *session;
    WT_CURSOR *cursor;
} WterlCursorHandle;

struct wterl_event_handlers {
    WT_EVENT_HANDLER handlers;
    ErlNifEnv *msg_env_error;
    ErlNifMutex *error_mutex;
    ErlNifEnv *msg_env_message;
    ErlNifMutex *message_mutex;
    ErlNifEnv *msg_env_progress;
    ErlNifMutex *progress_mutex;
    ErlNifPid to_pid;
};

struct wterl_priv_data {
    void *async_nif_priv; // Note: must be first element in struct
    struct wterl_event_handlers eh;
    char wterl_vsn[512];
    char wiredtiger_vsn[512];
};

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_FIRST;
static ERL_NIF_TERM ATOM_LAST;
static ERL_NIF_TERM ATOM_MESSAGE;
static ERL_NIF_TERM ATOM_PROGRESS;
static ERL_NIF_TERM ATOM_WTERL_VSN;
static ERL_NIF_TERM ATOM_WIREDTIGER_VSN;
static ERL_NIF_TERM ATOM_MSG_PID;

/* Global init for async_nif. */
ASYNC_NIF_INIT(wterl);

static inline size_t
__strlen(const char *s)
{
    if (s)
        return strlen(s);
    else
        return 0;
}

/**
 * A string hash function.
 *
 * A basic hash function for strings of characters used during the
 * affinity association.
 *
 * s    a NULL terminated set of bytes to be hashed
 * ->   an integer hash encoding of the bytes
 */
static inline uint32_t
__str_hash(uint32_t in, const char *p, size_t len)
{
    uint32_t h = in;
    for (++p ; len > 0; ++p, --len)
        h += (h << 5) + (h >> 27) + *p;
    return h;
}

#if defined(__amd64) || defined(__x86_64)
/* Note: we'll use this to lower the chances that we'll have a hash
   collision until I can finish a nice trie and use that to be a bit
   more precise.  When that's done we can skip hash/crc32 and just
   use the binary position in the trie as our "signature". */
static inline uint32_t
__crc32(uint32_t crc, const char *bytes, size_t len)
{
    const uint8_t *p;
    for (p = (const uint8_t*)bytes; len > 0; ++p, --len) {
        __asm__ __volatile__(
            ".byte 0xF2, 0x0F, 0x38, 0xF0, 0xF1"
            : "=S" (crc)
            : "0" (crc), "c" (*p));
    }
    return crc;
}
#else
#error unsupported platform
#endif

/**
 * Calculate the log2 of 64bit unsigned integers.
 */
#ifdef __GCC__
#define __log2(X) ((unsigned) ((8 * (sizeof(uint64_t) - 1))  - __builtin_clzll((X))))
#else
static inline uint32_t __log2(uint64_t x) {
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
#endif

/**
 * Evict items from the cache.
 *
 * Evict old contexts from the cache to make space for new, more frequently
 * used contexts.
 *
 * ->   number of items evicted
 */
static int
__ctx_cache_evict(WterlConnHandle *conn_handle)
{
    uint32_t mean, log, num_evicted, i;
    uint64_t now, elapsed;
    struct wterl_ctx *c, *n;

    if (conn_handle->cache_size < MAX_CACHE_SIZE)
        return 0;

    now = cpu_clock_ticks();

    // Find the mean of the recorded times that items stayed in cache.
    mean = 0;
    for (i = 0; i < 64; i++)
        mean += (conn_handle->histogram[i] * i);
    if (mean > 0)
        mean /= conn_handle->histogram_count;

    // Clear out the histogram and hit/misses
    memset(conn_handle->histogram, 0, sizeof(uint64_t) * 64);
    conn_handle->histogram_count = 0;

    /*
     * Evict anything older than the mean time in queue by removing those
     * items from the lists stored in the tree.
     */
    num_evicted = 0;
    c = STAILQ_FIRST(&conn_handle->cache);
    while (c != NULL) {
        n = STAILQ_NEXT(c, entries);
        elapsed = c->tstamp - now;
        log = __log2(elapsed);
        if (log > mean) {
            STAILQ_REMOVE(&conn_handle->cache, c, wterl_ctx, entries);
            DPRINTF("evicting: %llu", PRIuint64(c->sig));
            c->session->close(c->session, NULL);
            enif_free(c);
            num_evicted++;
        }
        c = n;
    }
    conn_handle->cache_size -= num_evicted;
    return num_evicted;
}

/**
 * Find a matching item in the cache.
 *
 * See if there exists an item in the cache with a matching signature, if
 * so remove it from the cache and return it for use by the callee.
 *
 * sig  a 64-bit signature (hash) representing the combination of Uri and
 *      session+config/cursor+config pairs needed for this operation
 */
static struct wterl_ctx *
__ctx_cache_find(WterlConnHandle *conn_handle, const uint64_t sig)
{
    struct wterl_ctx *c;

    enif_mutex_lock(conn_handle->cache_mutex);
    c = STAILQ_FIRST(&conn_handle->cache);
    while (c != NULL) {
        if (c->sig == sig) { // TODO: hash collisions *will* lead to SEGVs
            // cache hit:
            STAILQ_REMOVE(&conn_handle->cache, c, wterl_ctx, entries);
            conn_handle->histogram[__log2(cpu_clock_ticks() - c->tstamp)]++;
            conn_handle->histogram_count++;
            conn_handle->cache_size -= 1;
            break;
        }
        c = STAILQ_NEXT(c, entries);
    }
#ifdef DEBUG
    uint32_t sz = 0;
    struct wterl_ctx *f;
    STAILQ_FOREACH(f, &conn_handle->cache, entries) {
	sz++;
    }
#endif
    enif_mutex_unlock(conn_handle->cache_mutex);
    DPRINTF("cache_find: [%u:%u] %s (%p)", sz, conn_handle->cache_size, c ? "hit" : "miss", c);
    return c;
}

/**
 * Add/Return an item to the cache.
 *
 * Return an item into the cache, reset the cursors it has open and put it at
 * the front of the LRU.
 */
static void
__ctx_cache_add(WterlConnHandle *conn_handle, struct wterl_ctx *c)
{
    enif_mutex_lock(conn_handle->cache_mutex);
    __ctx_cache_evict(conn_handle);
    c->tstamp = cpu_clock_ticks();
    STAILQ_INSERT_TAIL(&conn_handle->cache, c, entries);
    conn_handle->cache_size += 1;
#ifdef DEBUG
    uint32_t sz = 0;
    struct wterl_ctx *f;
    STAILQ_FOREACH(f, &conn_handle->cache, entries) {
	sz++;
    }
#endif
    enif_mutex_unlock(conn_handle->cache_mutex);
    DPRINTF("cache_add: [%u:%u] (%p)", sz, conn_handle->cache_size, c);
}

static inline char *
__copy_str_into(char **p, const char *s)
{
    char *a = *p;
    size_t len =  __strlen(s);
    memcpy(*p, s, len);
    (*p)[len] = '\0';
    *p += len + 1;
    return a;
}

/**
 * Get a reusable cursor that was opened for a particular worker within its
 * session.
 */
static int
__retain_ctx(WterlConnHandle *conn_handle, uint32_t worker_id,
             struct wterl_ctx **ctx,
             int count, const char *session_config, ...)
{
    int i = 0;
    uint32_t hash = 0;
    uint32_t crc = 0;
    uint64_t sig = 0;
    size_t l, sig_len = 0;
    va_list ap;
    const char *arg;
    struct wterl_ctx *c;

    arg = session_config;
    va_start(ap, session_config);
    if (session_config) {
        l = __strlen(session_config);
        hash = __str_hash(hash, session_config, l);
        crc = __crc32(crc, session_config, l);
        sig_len += l + 1;
	DPRINTF("sig/1: %s", session_config);
    } else {
        sig_len += 1;
    }
    for (i = 0; i < (2 * count); i++) {
        arg = va_arg(ap, const char *);
        if (arg) {
            l = __strlen(arg);
	    DPRINTF("sig/args: %s", arg);
            hash = __str_hash(hash, arg, l);
            crc = __crc32(crc, arg, l);
            sig_len += l + 1;
        } else {
            sig_len += 1;
        }
    }
    sig = (uint64_t)crc << 32 | hash;
    DPRINTF("sig %llu [%u:%u]", PRIuint64(sig), crc, hash);
    va_end(ap);

    *ctx = NULL;

    c = conn_handle->mru_ctx[worker_id];
    if (CASPO(&conn_handle->mru_ctx[worker_id], c, 0) == c) {
	if (c == 0) {
	    // mru miss:
	    DPRINTF("[%.4u] mru miss, empty", worker_id);
	    *ctx = NULL;
	} else {
	    if (c->sig == sig) {
		// mru hit:
		DPRINTF("[%.4u] mru hit: %llu found", worker_id, PRIuint64(sig));
		*ctx = c;
	    } else {
		// mru mismatch:
		DPRINTF("[%.4u] mru miss: %llu != %llu", worker_id, PRIuint64(sig), PRIuint64(c->sig));
		__ctx_cache_add(conn_handle, c);
		*ctx = NULL;
	    }
        }
    }

    if (*ctx == NULL) {
        // check the cache
        (*ctx) = __ctx_cache_find(conn_handle, sig);
        if ((*ctx) == NULL) {
            // cache miss:
            DPRINTF("[%.4u] cache miss: %llu [cache size: %d]", worker_id, PRIuint64(sig), conn_handle->cache_size);
            WT_CONNECTION *conn = conn_handle->conn;
            WT_SESSION *session = NULL;
            int rc = conn->open_session(conn, NULL, session_config, &session);
            if (rc != 0) {
                return rc;
            }
            size_t s = sizeof(struct wterl_ctx) + (count * sizeof(struct cursor_info)) + sig_len;
            *ctx = enif_alloc(s); // TODO: enif_alloc_resource()
            if (*ctx == NULL) {
                session->close(session, NULL);
                return ENOMEM;
            }
            memset(*ctx, 0, s);
            (*ctx)->sig = sig;
            (*ctx)->session = session;
            (*ctx)->sig_len = sig_len;
            char *p = (char *)(*ctx) + (s - sig_len);
            (*ctx)->session_config = __copy_str_into(&p, session_config);
            (*ctx)->num_cursors = count;
            session_config = arg;
            va_start(ap, session_config);
            for (i = 0; i < count; i++) {
                const char *uri = va_arg(ap, const char *);
                const char *config = va_arg(ap, const char *);
                // TODO: what to do (if anything) when uri or config is NULL?
                (*ctx)->ci[i].uri = __copy_str_into(&p, uri);
                (*ctx)->ci[i].config = __copy_str_into(&p, config);
                rc = session->open_cursor(session, uri, NULL, config, &(*ctx)->ci[i].cursor);
                if (rc != 0) {
                    enif_free(*ctx);
                    session->close(session, NULL); // this will free the cursors too
                    return rc;
                }
            }
            va_end(ap);
        } else {
            // cache hit:
            DPRINTF("[%.4u] cache hit: %llu [cache size: %d]", worker_id, PRIuint64(sig), conn_handle->cache_size);
        }
    }
    return 0;
}

/**
 * Return a context to the cache for reuse.
 */
static void
__release_ctx(WterlConnHandle *conn_handle, uint32_t worker_id, struct wterl_ctx *ctx)
{
    uint32_t i;
    WT_CURSOR *cursor;
    struct wterl_ctx *c = NULL;

    for (i = 0; i < ctx->num_cursors; i++) {
        cursor = ctx->ci[i].cursor;
        cursor->reset(cursor);
    }

    c = conn_handle->mru_ctx[worker_id];
    if (CASPO(&conn_handle->mru_ctx[worker_id], c, ctx) != c) {
        __ctx_cache_add(conn_handle, ctx);
        DPRINTF("[%.4u] reset %d cursors, returnd ctx to cache", worker_id, ctx->num_cursors);
    } else {
        if (c != NULL) {
            __ctx_cache_add(conn_handle, c);
            DPRINTF("[%.4u] reset %d cursors, returned ctx to cache", worker_id, ctx->num_cursors);
        } else {
            DPRINTF("[%.4u] reset %d cursors, returned ctx to mru", worker_id, ctx->num_cursors);
        }
    }
}

/**
 * Close all sessions and all cursors open on any objects.
 *
 * Note: always call within enif_mutex_lock/unlock(conn_handle->cache_mutex)
 */
void
__close_all_sessions(WterlConnHandle *conn_handle)
{
    struct wterl_ctx *c, *n;
    int worker_id;

    // clear out the mru
    for (worker_id = 0; worker_id < ASYNC_NIF_MAX_WORKERS; worker_id++) {
        do {
            c = conn_handle->mru_ctx[worker_id];
        } while(CASPO(&conn_handle->mru_ctx[worker_id], c, 0) != c);

        if (c != 0) {
            c->session->close(c->session, NULL);
            enif_free(c);
        }
    }

    // clear out the cache
    c = STAILQ_FIRST(&conn_handle->cache);
    while (c != NULL) {
        n = STAILQ_NEXT(c, entries);
        STAILQ_REMOVE(&conn_handle->cache, c, wterl_ctx, entries);
        conn_handle->cache_size -= 1;
        c->session->close(c->session, NULL);
        enif_free(c);
        c = n;
    }
}

/**
 * Close cursors open on 'uri' object.
 *
 * Note: always call within enif_mutex_lock/unlock(conn_handle->cache_mutex)
 */
void
__close_cursors_on(WterlConnHandle *conn_handle, const char *uri)
{
    struct wterl_ctx *c, *n;
    int worker_id, idx, cnt;

    // walk the mru first, look for open cursors on matching uri
    for (worker_id = 0; worker_id < ASYNC_NIF_MAX_WORKERS; worker_id++) {
        c = conn_handle->mru_ctx[worker_id];
        if (CASPO(&conn_handle->mru_ctx[worker_id], c, 0) == c && c != 0) {
            cnt = c->num_cursors;
            for(idx = 0; idx < cnt; idx++) {
                if (!strcmp(c->ci[idx].uri, uri)) {
                    c->session->close(c->session, NULL);
                    enif_free(c);
                    break;
                } else {
                    if (CASPO(&conn_handle->mru_ctx[worker_id], 0, c) != 0) {
                        __ctx_cache_add(conn_handle, c);
                    }
                }
            }
        }
    }

    // next we walk the cache, look for open cursors on matching uri
    c = STAILQ_FIRST(&conn_handle->cache);
    while (c != NULL) {
        n = STAILQ_NEXT(c, entries);
        cnt = c->num_cursors;
        for(idx = 0; idx < cnt; idx++) {
            if (!strcmp(c->ci[idx].uri, uri)) {
                STAILQ_REMOVE(&conn_handle->cache, c, wterl_ctx, entries);
                conn_handle->cache_size -= 1;
                c->session->close(c->session, NULL);
                enif_free(c);
                break;
            }
        }
        c = n;
    }
    return;
}

/**
 * Callback to handle error messages.
 *
 * Deliver error messages into Erlang to be logged via loger:error()
 * or on failure, write message to stderr.
 *
 * error    a WiredTiger, C99 or POSIX error code, which can
 *          be converted to a string using wiredtiger_strerror()
 * message  an error string
 * ->       0 on success, a non-zero return may cause the WiredTiger
 *          function posting the event to fail, and may even cause
 *          operation or library failure.
 */
int
__wterl_error_handler(WT_EVENT_HANDLER *handler, int error, const char *message)
{
    struct wterl_event_handlers *eh = (struct wterl_event_handlers *)handler;
    ErlNifEnv *msg_env;
    ErlNifPid *to_pid;
    int rc = 0;

    enif_mutex_lock(eh->error_mutex);
    msg_env = eh->msg_env_error;
    to_pid = &eh->to_pid;
    if (msg_env) {
        ERL_NIF_TERM msg =
            enif_make_tuple2(msg_env, ATOM_ERROR,
                 enif_make_tuple2(msg_env,
                      enif_make_atom(msg_env, erl_errno_id(error)),
                      enif_make_string(msg_env, message, ERL_NIF_LATIN1)));
        enif_clear_env(msg_env);
        if (!enif_send(NULL, to_pid, msg_env, msg))
           fprintf(stderr, "[%d] %s\n", error, message);
    } else {
        rc = (fprintf(stderr, "[%d] %s\n", error, message) >= 0 ? 0 : EIO);
    }
    enif_mutex_unlock(eh->error_mutex);
    return rc;
}

/**
 * Callback to handle informational messages.
 *
 * Deliver informational messages into Erlang to be logged via loger:info()
 * or on failure, write message to stdout.
 *
 * message  an informational string
 * ->       0 on success, a non-zero return may cause the WiredTiger
 *          function posting the event to fail, and may even cause
 *          operation or library failure.
 */
int
__wterl_message_handler(WT_EVENT_HANDLER *handler, const char *message)
{
    struct wterl_event_handlers *eh = (struct wterl_event_handlers *)handler;
    ErlNifEnv *msg_env;
    ErlNifPid *to_pid;
    int rc = 0;

    enif_mutex_lock(eh->message_mutex);
    msg_env = eh->msg_env_message;
    to_pid = &eh->to_pid;
    if (msg_env) {
        ERL_NIF_TERM msg =
            enif_make_tuple2(msg_env, ATOM_MESSAGE,
                 enif_make_string(msg_env, message, ERL_NIF_LATIN1));
        enif_clear_env(msg_env);
        if (!enif_send(NULL, to_pid, msg_env, msg))
            fprintf(stderr, "%s\n", message);
    } else {
        rc = (printf("%s\n", message) >= 0 ? 0 : EIO);
    }
    enif_mutex_unlock(eh->message_mutex);
    return rc;
}

/**
 * Callback to handle progress messages.
 *
 * Deliver progress messages into Erlang to be logged via loger:info()
 * or on failure, written message to stdout.
 *
 * operation    a string representation of the operation
 * counter      a progress counter [0..100]
 * ->       0 on success, a non-zero return may cause the WiredTiger
 *          function posting the event to fail, and may even cause
 *          operation or library failure.
 */
int
__wterl_progress_handler(WT_EVENT_HANDLER *handler, const char *operation, uint64_t counter)
{
    struct wterl_event_handlers *eh = (struct wterl_event_handlers *)handler;
    ErlNifEnv *msg_env;
    ErlNifPid *to_pid;
    int rc = 0;

    enif_mutex_lock(eh->progress_mutex);
    msg_env = eh->msg_env_progress;
    to_pid = &eh->to_pid;
    if (msg_env) {
        ERL_NIF_TERM msg =
            enif_make_tuple2(msg_env, ATOM_PROGRESS,
                 enif_make_tuple2(msg_env,
                      enif_make_string(msg_env, operation, ERL_NIF_LATIN1),
                      enif_make_int64(msg_env, counter)));
        enif_clear_env(msg_env);
        if (!enif_send(NULL, to_pid, msg_env, msg))
            fprintf(stderr, "[%llu] %s\n", PRIuint64(counter), operation);
    } else {
        rc = (printf("[%llu] %s\n", PRIuint64(counter), operation) >= 0 ? 0 : EIO);
    }
    enif_mutex_unlock(eh->progress_mutex);
    return rc;
}

/**
 * Convenience function to generate {error, {errno, Reason}} or 'not_found'
 * Erlang terms to return to callers.
 *
 * env    NIF environment
 * rc     code returned by WiredTiger
 */
static ERL_NIF_TERM
__strerror_term(ErlNifEnv* env, int rc)
{
    if (rc == WT_NOTFOUND) {
        return ATOM_NOT_FOUND;
    } else {
        /* We return the errno value as well as the message here because the
           error message provided by strerror() for differ across platforms
           and/or may be localized to any given language (i18n).  Use the errno
           atom rather than the message when matching in Erlang.  You've been
           warned. */
        DPRINTF("error: %s", erl_errno_id(rc));
        return enif_make_tuple2(env, ATOM_ERROR,
                    enif_make_tuple2(env,
                         enif_make_atom(env, erl_errno_id(rc)),
                         enif_make_string(env, wiredtiger_strerror(rc), ERL_NIF_LATIN1)));
    }
}

/**
 * Opens a WiredTiger WT_CONNECTION object.
 *
 * argv[0]    path to directory for the database files
 * argv[1]    WiredTiger connection config string as an Erlang binary
 * argv[2]    WiredTiger session config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_conn_open,
  { // struct

    ERL_NIF_TERM config;
    ERL_NIF_TERM session_config;
    char homedir[4096];
    struct wterl_priv_data *priv;
  },
  { // pre

    if (!(argc == 3 &&
          (enif_get_string(env, argv[0], args->homedir, sizeof(args->homedir), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[1]) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    args->session_config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);

    args->priv = (struct wterl_priv_data *)enif_priv_data(env);
  },
  { // work

    WT_CONNECTION *conn;
    ErlNifBinary config;
    ErlNifBinary session_config;

    if (!enif_inspect_binary(env, args->config, &config)) {
        ASYNC_NIF_REPLY(enif_make_badarg(env));
        return;
    }
    if (!enif_inspect_binary(env, args->session_config, &session_config)) {
        ASYNC_NIF_REPLY(enif_make_badarg(env));
        return;
    }

    int rc = wiredtiger_open(args->homedir,
                             (WT_EVENT_HANDLER*)&args->priv->eh.handlers,
                             (config.size > 1) ? (const char *)config.data : NULL,
                             &conn);
    if (rc == 0) {
      WterlConnHandle *conn_handle = enif_alloc_resource(wterl_conn_RESOURCE, sizeof(WterlConnHandle));
      memset(conn_handle, 0, sizeof(WterlConnHandle));
      if (!conn_handle) {
          ASYNC_NIF_REPLY(__strerror_term(env, ENOMEM));
          return;
      }
      if (session_config.size > 1) {
          char *sc = enif_alloc(session_config.size);
          if (!sc) {
              enif_release_resource(conn_handle);
              ASYNC_NIF_REPLY(__strerror_term(env, ENOMEM));
              return;
          }
          memcpy(sc, session_config.data, session_config.size);
          conn_handle->session_config = (const char *)sc;
      } else {
          conn_handle->session_config = NULL;
      }
      conn_handle->cache_mutex = enif_mutex_create(NULL);
      enif_mutex_lock(conn_handle->cache_mutex);
      conn_handle->conn = conn;
      ERL_NIF_TERM result = enif_make_resource(env, conn_handle);

      /* Init list for cache of reuseable contexts */
      STAILQ_INIT(&conn_handle->cache);
      conn_handle->cache_size = 0;

      enif_release_resource(conn_handle);
      enif_mutex_unlock(conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
    }
    else
    {
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
    }
  },
  { // post

  });

/**
 * Closes a WiredTiger WT_CONNECTION object.
 *
 * argv[0]    WterlConnHandle resource
 */
ASYNC_NIF_DECL(
  wterl_conn_close,
  { // struct

    WterlConnHandle* conn_handle;
    struct wterl_priv_data *priv;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->conn_handle);

    args->priv = (struct wterl_priv_data *)enif_priv_data(env);
  },
  { // work

    /* Free up the shared sessions and cursors. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_all_sessions(args->conn_handle);
    if (args->conn_handle->session_config) {
        enif_free((char *)args->conn_handle->session_config);
        args->conn_handle->session_config = NULL;
    }
    WT_CONNECTION* conn = args->conn_handle->conn;
    int rc = conn->close(conn, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    enif_mutex_destroy(args->conn_handle->cache_mutex);
    memset(args->conn_handle, 0, sizeof(WterlConnHandle));

    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Create a WiredTiger table, column group, index or file.
 *
 * We create, use and discard a WT_SESSION here because table creation is not
 * too performance sensitive.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_create,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create, use and discard a WT_SESSION here because a) we don't need a
       cursor and b) we don't anticipate doing this operation frequently enough
       to impact performance. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }

    rc = session->create(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Drop (remove) a WiredTiger table, column group, index or file.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_drop,
  { // struct

    Uri uri;
    ERL_NIF_TERM config;
    WterlConnHandle *conn_handle;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create, use and discard a WT_SESSION here because a) we don't need a
       cursor and b) we don't anticipate doing this operation frequently enough
       to impact performance. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
        enif_mutex_unlock(args->conn_handle->cache_mutex);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    /* Note: we locked the context mutex and called __close_cursors_on()
       earlier so that we are sure that before we call into WiredTiger we have
       first closed all open cursors referencing this object.  Failure to do
       this will result in EBUSY(16) "Device or resource busy". */
    rc = session->drop(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Rename a WiredTiger table, column group, index or file.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    old object name URI string
 * argv[2]    new object name URI string
 * argv[3]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_rename,
  { // struct

    Uri oldname;
    Uri newname;
    ERL_NIF_TERM config;
    WterlConnHandle *conn_handle;
  },
  { // pre

    if (!(argc == 4 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->oldname, sizeof(args->oldname), ERL_NIF_LATIN1) > 0) &&
          (enif_get_string(env, argv[2], args->newname, sizeof(args->newname), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[3]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_cursors_on(args->conn_handle, args->oldname);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create, use and discard a WT_SESSION here because a) we don't need a
       cursor and b) we don't anticipate doing this operation frequently enough
       to impact performance. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
      return;
    }

    /* Note: we locked the context mutex and called __close_cursors_on()
       earlier so that we are sure that before we call into WiredTiger we have
       first closed all open cursors referencing this object.  Failure to do
       this will result in EBUSY(16) "Device or resource busy". */
    rc = session->rename(session, args->oldname, args->newname, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });


/**
 * Salvage rebuilds the file, or files of which a table is comprised,
 * discarding any corrupted file blocks.  Use this as a fallback if
 * an attempt to open a WiredTiger database object fails.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_salvage,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create, use and discard a WT_SESSION here because a) we don't need a
       cursor and b) we don't anticipate doing this operation frequently enough
       to impact performance. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
      return;
    }

    rc = session->salvage(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Checkpoint writes a transactionally consistent snapshot of a database or set
 * of objects specified.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_checkpoint,
  { // struct

    WterlConnHandle *conn_handle;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 2 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          enif_is_binary(env, argv[1]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    rc = session->checkpoint(session, (const char*)config.data);
    (void)session->close(session, NULL);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Truncate a file, table or cursor range.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    start key as an Erlang binary
 * argv[3]    stop key as an Erlang binary
 * argv[4]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_truncate,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    int from_first;
    int to_last;
    ERL_NIF_TERM start;
    ERL_NIF_TERM stop;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 5 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[4]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    if (enif_is_binary(env, argv[2])) {
        args->from_first = 0;
        args->start = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    } else if (enif_is_atom(env, argv[2])) { // TODO && argv[2] == ATOM_FIRST) {
        args->from_first = 1;
        args->start = 0;
    } else {
        ASYNC_NIF_RETURN_BADARG();
    }
    if (enif_is_binary(env, argv[3])) {
        args->to_last = 0;
        args->stop = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    } else if (enif_is_atom(env, argv[3])) { // TODO && argv[3] == ATOM_LAST) {
        args->to_last = 1;
        args->stop = 0;
    } else {
        ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[4]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* Note: we locked the context mutex and called __close_cursors_on()
       earlier so that we are sure that before we call into WiredTiger we have
       first closed all open cursors referencing this object.  Failure to do
       this will result in EBUSY(16) "Device or resource busy". */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
        enif_mutex_unlock(args->conn_handle->cache_mutex);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }

    ErlNifBinary start_key;
    ErlNifBinary stop_key;
    WT_CURSOR *start = NULL;
    WT_CURSOR *stop = NULL;

    /* The truncate method should be passed either a URI or start/stop cursors,
       but not both.  So we simply open cursors no matter what to avoid the
       mess. */
    if (!args->from_first) {
        if (!enif_inspect_binary(env, args->start, &start_key)) {
            enif_mutex_unlock(args->conn_handle->cache_mutex);
            ASYNC_NIF_REPLY(enif_make_badarg(env));
            return;
        }
    }
    rc = session->open_cursor(session, args->uri, NULL, "raw", &start);
    if (rc != 0) {
        session->close(session, NULL);
        enif_mutex_unlock(args->conn_handle->cache_mutex);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    /* Position the start cursor at the first record or the specified record. */
    if (args->from_first) {
        rc = start->next(start);
        if (rc != 0) {
            start->close(start);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->cache_mutex);
            ASYNC_NIF_REPLY(__strerror_term(env, rc));
            return;
        }
    } else {
        WT_ITEM item_start;
        item_start.data = start_key.data;
        item_start.size = start_key.size;
        start->set_key(start, item_start);
    }

    if (!args->to_last) {
        if (!enif_inspect_binary(env, args->stop, &stop_key)) {
            start->close(start);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->cache_mutex);
            ASYNC_NIF_REPLY(enif_make_badarg(env));
            return;
        }
    }
    rc = session->open_cursor(session, args->uri, NULL, "raw", &stop);
    if (rc != 0) {
        start->close(start);
        session->close(session, NULL);
        enif_mutex_unlock(args->conn_handle->cache_mutex);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    /* Position the stop cursor at the last record or the specified record. */
    if (args->to_last) {
        rc = stop->prev(stop);
        if (rc != 0) {
            start->close(start);
            stop->close(stop);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->cache_mutex);
            ASYNC_NIF_REPLY(__strerror_term(env, rc));
            return;
        }
    } else {
        WT_ITEM item_stop;
        item_stop.data = stop_key.data;
        item_stop.size = stop_key.size;
        stop->set_key(stop, item_stop);
    }

    /* Always pass NULL for URI here because we always specify the range with the
       start and stop cursors which were opened referencing that URI. */
    rc = session->truncate(session, NULL, start, stop, (const char*)config.data);

    start->close(start);
    stop->close(stop);
    session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Upgrade upgrades a file or table, if upgrade is required.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_upgrade,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create, use and discard a WT_SESSION here because a) we don't need a
       cursor and b) we don't anticipate doing this operation frequently enough
       to impact performance. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
        enif_mutex_unlock(args->conn_handle->cache_mutex);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }

    rc = session->upgrade(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Verify reports if a file, or the files of which a table is comprised, have
 * been corrupted.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_verify,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->cache_mutex);
    __close_all_sessions(args->conn_handle);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      enif_mutex_unlock(args->conn_handle->cache_mutex);
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create, use and discard a WT_SESSION here because a) we don't need a
       cursor and b) we don't anticipate doing this operation frequently enough
       to impact performance. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
        enif_mutex_unlock(args->conn_handle->cache_mutex);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }

    rc = session->verify(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->cache_mutex);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Delete a key's value from the specified table or index.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    key as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_delete,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM key;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
    //affinity = __str_hash(0, args->uri, __strlen(args->uri));
  },
  { // work

    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    struct wterl_ctx *ctx = NULL;
    WT_CURSOR *cursor = NULL;
    int rc = __retain_ctx(args->conn_handle, worker_id, &ctx, 1,
                          args->conn_handle->session_config,
                          args->uri, "overwrite,raw");
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    cursor = ctx->ci[0].cursor;

    WT_ITEM item_key;
    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    rc = cursor->remove(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
    __release_ctx(args->conn_handle, worker_id, ctx);
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Get the value for the key's value from the specified table or index.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    key as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_get,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM key;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
    //affinity = __str_hash(0, args->uri, __strlen(args->uri));
  },
  { // work

    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    struct wterl_ctx *ctx = NULL;
    WT_CURSOR *cursor = NULL;
    int rc = __retain_ctx(args->conn_handle, worker_id, &ctx, 1,
                          args->conn_handle->session_config,
                          args->uri, "overwrite,raw");
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    cursor = ctx->ci[0].cursor;

    WT_ITEM item_key;
    WT_ITEM item_value;
    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    rc = cursor->search(cursor);
    if (rc != 0) {
        __release_ctx(args->conn_handle, worker_id, ctx);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }

    rc = cursor->get_value(cursor, &item_value);
    if (rc != 0) {
        __release_ctx(args->conn_handle, worker_id, ctx);
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }

    ERL_NIF_TERM value;
    unsigned char *bin = enif_make_new_binary(env, item_value.size, &value);
    memcpy(bin, item_value.data, item_value.size);
    ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, value));
    __release_ctx(args->conn_handle, worker_id, ctx);
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Store a value for the key's value from the specified table or index.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    key as an Erlang binary
 * argv[3]    value as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_put,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM key;
    ERL_NIF_TERM value;
  },
  { // pre

    if (!(argc == 4 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]) &&
          enif_is_binary(env, argv[3]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    args->value = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    enif_keep_resource((void*)args->conn_handle);
    //affinity = __str_hash(0, args->uri, __strlen(args->uri));
  },
  { // work

    ErlNifBinary key;
    ErlNifBinary value;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    if (!enif_inspect_binary(env, args->value, &value)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    struct wterl_ctx *ctx = NULL;
    WT_CURSOR *cursor = NULL;
    int rc = __retain_ctx(args->conn_handle, worker_id, &ctx, 1,
                          args->conn_handle->session_config,
                          args->uri, "overwrite,raw");
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        return;
    }
    cursor = ctx->ci[0].cursor;

    WT_ITEM item_key;
    WT_ITEM item_value;
    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    item_value.data = value.data;
    item_value.size = value.size;
    cursor->set_value(cursor, &item_value);
    rc = cursor->insert(cursor);
    __release_ctx(args->conn_handle, worker_id, ctx);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Open a cursor on a table or index.
 *
 * argv[0]    WterlConnHandle resource
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_cursor_open,
  { // struct

    WterlConnHandle *conn_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle) &&
          (enif_get_string(env, argv[1], args->uri, sizeof(args->uri), ERL_NIF_LATIN1) > 0) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    /* We create a separate session here to ensure that operations are thread safe. */
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
      return;
    }

    WT_CURSOR* cursor;
    rc = session->open_cursor(session, args->uri, NULL, (config.data[0] != 0) ? (char *)config.data : "overwrite,raw", &cursor);
    if (rc != 0) {
      session->close(session, NULL);
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
      return;
    }

    WterlCursorHandle* cursor_handle = enif_alloc_resource(wterl_cursor_RESOURCE, sizeof(WterlCursorHandle));
    if (!cursor_handle) {
      cursor->close(cursor);
      session->close(session, NULL);
      ASYNC_NIF_REPLY(__strerror_term(env, ENOMEM));
      return;
    }
    memset(cursor_handle, 0, sizeof(WterlCursorHandle));
    cursor_handle->session = session;
    cursor_handle->cursor = cursor;
    ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
    enif_release_resource(cursor_handle);
    ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

/**
 * Close a cursor releasing resources it held.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_close,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    /* Note: session->close() will cause all open cursors in the session to be
       closed first, so we don't have explicitly to do that here.

       WT_CURSOR *cursor = args->cursor_handle->cursor;
       rc = cursor->close(cursor);
     */
    WT_SESSION *session = args->cursor_handle->session;
    int rc = session->close(session, NULL);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

static ERL_NIF_TERM
__cursor_key_ret(ErlNifEnv *env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0) {
        WT_ITEM item_key;
        rc = cursor->get_key(cursor, &item_key);
        if (rc == 0) {
            ERL_NIF_TERM key;
            memcpy(enif_make_new_binary(env, item_key.size, &key), item_key.data, item_key.size);
            return enif_make_tuple2(env, ATOM_OK, key);
        }
    }
    return __strerror_term(env, rc);
}

static ERL_NIF_TERM
__cursor_kv_ret(ErlNifEnv *env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0) {
        WT_ITEM item_key, item_value;
        rc = cursor->get_key(cursor, &item_key);
        if (rc == 0) {
            rc = cursor->get_value(cursor, &item_value);
            if (rc == 0) {
                ERL_NIF_TERM key, value;
                memcpy(enif_make_new_binary(env, item_key.size, &key), item_key.data, item_key.size);
                memcpy(enif_make_new_binary(env, item_value.size, &value), item_value.data, item_value.size);
                return enif_make_tuple3(env, ATOM_OK, key, value);
            }
        }
    }
    return __strerror_term(env, rc);
}

static ERL_NIF_TERM
__cursor_value_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0) {
        WT_ITEM item_value;
        rc = cursor->get_value(cursor, &item_value);
        if (rc == 0) {
            ERL_NIF_TERM value;
            memcpy(enif_make_new_binary(env, item_value.size, &value), item_value.data, item_value.size);
            return enif_make_tuple2(env, ATOM_OK, value);
        }
    }
    return __strerror_term(env, rc);
}

/**
 * Use a cursor to fetch the next key/value pair from the table or index.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_next,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ASYNC_NIF_REPLY(__cursor_kv_ret(env, cursor, cursor->next(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Use a cursor to fetch the next key from the table or index.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_next_key,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ASYNC_NIF_REPLY(__cursor_key_ret(env, cursor, cursor->next(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Use a cursor to fetch the next value from the table or index.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_next_value,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ASYNC_NIF_REPLY(__cursor_value_ret(env, cursor, cursor->next(cursor)));
    DPRINTF("env: %p cursor: %p", env, cursor);
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Use a cursor to fetch the previous key/value pair from the table or index.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_prev,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ASYNC_NIF_REPLY(__cursor_kv_ret(env, cursor, cursor->prev(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Use a cursor to fetch the previous key from the table or index.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_prev_key,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ASYNC_NIF_REPLY(__cursor_key_ret(env, cursor, cursor->prev(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Use a cursor to fetch the previous value from the table or index.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_prev_value,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ASYNC_NIF_REPLY(__cursor_value_ret(env, cursor, cursor->prev(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Position the cursor at the record matching the key.
 *
 * argv[0]    WterlCursorHandle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    boolean, when false the cursor will be reset
 */
ASYNC_NIF_DECL(
  wterl_cursor_search,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
    int scanning;
  },
  { // pre

    static ERL_NIF_TERM ATOM_TRUE = 0;
    if (ATOM_TRUE == 0)
        enif_make_atom(env, "true");

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle) &&
          enif_is_binary(env, argv[1]) &&
          enif_is_atom(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    args->scanning = (enif_is_identical(argv[2], ATOM_TRUE)) ? 1 : 0;
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    WT_ITEM item_key;
    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);

    ERL_NIF_TERM reply = __cursor_value_ret(env, cursor, cursor->search(cursor));
    if (!args->scanning)
      (void)cursor->reset(cursor);
    ASYNC_NIF_REPLY(reply);
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Position the cursor at the record matching the key if it exists, or a record
 * that would be adjacent.
 *
 * argv[0]    WterlCursorHandle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    boolean, when false the cursor will be reset
 */
ASYNC_NIF_DECL(
  wterl_cursor_search_near,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
    int scanning;
  },
  { // pre

    static ERL_NIF_TERM ATOM_TRUE = 0;
    if (ATOM_TRUE == 0)
        enif_make_atom(env, "true");

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle) &&
          enif_is_binary(env, argv[1]) &&
          enif_is_atom(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    args->scanning = (enif_is_identical(argv[2], ATOM_TRUE)) ? 1 : 0;
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_ITEM item_key;
    int exact = 0;

    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);

    int rc = cursor->search_near(cursor, &exact);
    if (rc != 0) {
      (void)cursor->reset(cursor);
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
      return;
    }

    if (!args->scanning)
        (void)cursor->reset(cursor);

    if (exact == 0) {
      /* an exact match */
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_atom(env, "match")));
    } else if (exact < 0) {
      /* cursor now positioned at the next smaller key */
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_atom(env, "lt")));
    } else if (exact > 0) {
      /* cursor now positioned at the next larger key */
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, enif_make_atom(env, "gt")));
    }
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Reset the position of the cursor.
 *
 * Any resources held by the cursor are released, and the cursor's key and
 * position are no longer valid. A subsequent iteration with wterl_cursor_next
 * will move to the first record, or with wterl_cursor_prev will move to the
 * last record.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_reset,
  { // struct

    WterlCursorHandle *cursor_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    int rc = cursor->reset(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Insert, or overwrite, a record using a cursor.
 *
 * argv[0]    WterlCursorHandle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    value as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_cursor_insert,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
    ERL_NIF_TERM value;
    int rc;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle) &&
          enif_is_binary(env, argv[1]) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    args->value = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ErlNifBinary key;
    ErlNifBinary value;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    if (!enif_inspect_binary(env, args->value, &value)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_ITEM item_key;
    WT_ITEM item_value;

    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    item_value.data = value.data;
    item_value.size = value.size;
    cursor->set_value(cursor, &item_value);
    int rc = cursor->insert(cursor);
    if (rc == 0)
        rc = cursor->reset(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Update an existing record using a cursor.
 *
 * argv[0]    WterlCursorHandle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    value as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_cursor_update,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
    ERL_NIF_TERM value;
    int rc;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle) &&
          enif_is_binary(env, argv[1]) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    args->value = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ErlNifBinary key;
    ErlNifBinary value;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    if (!enif_inspect_binary(env, args->value, &value)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_ITEM item_key;
    WT_ITEM item_value;

    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    item_value.data = value.data;
    item_value.size = value.size;
    cursor->set_value(cursor, &item_value);
    int rc = cursor->update(cursor);
    if (rc == 0)
        rc = cursor->reset(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Remove a record using a cursor.
 *
 * argv[0]    WterlCursorHandle resource
 * argv[1]    key as an Erlang binary
 */
ASYNC_NIF_DECL(
  wterl_cursor_remove,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
    int rc;
  },
  { // pre

    if (!(argc == 2 &&
          enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&args->cursor_handle) &&
          enif_is_binary(env, argv[1]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    enif_keep_resource((void*)args->cursor_handle);
  },
  { // work

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_ITEM item_key;

    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    int rc = cursor->remove(cursor);
    if (rc == 0)
        rc = cursor->reset(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });


/**
 * Called by wterl_event_handler to set the pid for message delivery.
 */
static ERL_NIF_TERM
wterl_set_event_handler_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  struct wterl_priv_data *priv = enif_priv_data(env);
  struct wterl_event_handlers *eh = &priv->eh;

  if (!(argc == 1 && enif_is_pid(env, argv[0]))) {
      return enif_make_badarg(env);
  }
  if (enif_get_local_pid(env, argv[0], &eh->to_pid)) {
    if (!eh->msg_env_message)
      eh->msg_env_message = enif_alloc_env(); // TOOD: if (!eh->msg_env) { return ENOMEM; }
    if (!eh->msg_env_error)
      eh->msg_env_error = enif_alloc_env();
    if (!eh->msg_env_progress)
      eh->msg_env_progress = enif_alloc_env();

    eh->handlers.handle_error = __wterl_error_handler;
    eh->handlers.handle_message = __wterl_message_handler;
    eh->handlers.handle_progress = __wterl_progress_handler;
  } else {
    memset(&eh->to_pid, 0, sizeof(ErlNifPid));
  }
  return ATOM_OK;
}


/**
 * Called when a connection is free'd, our opportunity to clean up
 * allocated resources.
 */
static void __wterl_conn_dtor(ErlNifEnv* env, void* obj)
{
    UNUSED(env);
    WterlConnHandle *conn_handle = (WterlConnHandle *)obj;

    if (conn_handle->cache_mutex) {
        DPRINTF("Non-NULL conn_handle (%p) to free", obj);
        enif_mutex_lock(conn_handle->cache_mutex);
        __close_all_sessions(conn_handle);
        conn_handle->conn->close(conn_handle->conn, NULL);
        enif_mutex_unlock(conn_handle->cache_mutex);
        enif_mutex_destroy(conn_handle->cache_mutex);
    }
}


/**
 * Called as this driver is loaded by the Erlang BEAM runtime triggered by the
 * module's on_load directive.
 *
 * env        the NIF environment
 * priv_data  used to hold the state for this NIF rather than global variables
 * load_info  an Erlang term, in this case a list with two two-tuples of the
 *            form: [{wterl, ""}, {wiredtiger, ""}] where the strings contain
 *            the git hash of the last commit for this code.  This is used
 *            to determin if a rolling upgrade is possible or not.
 */
static int
on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    int arity;
    ERL_NIF_TERM head, tail;
    const ERL_NIF_TERM* option;
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    wterl_conn_RESOURCE = enif_open_resource_type(env, NULL, "wterl_conn_resource",
                                                  __wterl_conn_dtor, flags, NULL);
    wterl_cursor_RESOURCE = enif_open_resource_type(env, NULL, "wterl_cursor_resource",
                                                    NULL, flags, NULL);

    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
    ATOM_FIRST = enif_make_atom(env, "first");
    ATOM_LAST = enif_make_atom(env, "last");
    ATOM_MESSAGE = enif_make_atom(env, "message");
    ATOM_PROGRESS = enif_make_atom(env, "progress");
    ATOM_WTERL_VSN = enif_make_atom(env, "wterl_vsn");
    ATOM_WIREDTIGER_VSN = enif_make_atom(env, "wiredtiger_vsn");
    ATOM_MSG_PID = enif_make_atom(env, "message_pid");

    struct wterl_priv_data *priv = enif_alloc(sizeof(struct wterl_priv_data));
    if (!priv)
        return ENOMEM;
    memset(priv, 0, sizeof(struct wterl_priv_data));

    struct wterl_event_handlers *eh = &priv->eh;
    eh->error_mutex = enif_mutex_create(NULL);
    eh->message_mutex = enif_mutex_create(NULL);
    eh->progress_mutex = enif_mutex_create(NULL);

    /* Process the load_info array of tuples, we expect:
       [{wterl_vsn, "a version string"},
        {wiredtiger_vsn, "a version string"}]. */
    while (enif_get_list_cell(env, load_info, &head, &tail)) {
      if (enif_get_tuple(env, head, &arity, &option)) {
        if (arity == 2) {
          if (enif_is_identical(option[0], ATOM_WTERL_VSN)) {
            enif_get_string(env, option[1], priv->wterl_vsn, sizeof(priv->wterl_vsn), ERL_NIF_LATIN1);
          } else if (enif_is_identical(option[0], ATOM_WIREDTIGER_VSN)) {
            enif_get_string(env, option[1], priv->wiredtiger_vsn, sizeof(priv->wiredtiger_vsn), ERL_NIF_LATIN1);
          }
        }
      }
      load_info = tail;
    }

    /* Note: !!! the first element of our priv_data struct *must* be the
       pointer to the async_nif's private data which we set here. */
    ASYNC_NIF_LOAD(wterl, priv->async_nif_priv);
    if (!priv->async_nif_priv) {
        memset(priv, 0, sizeof(struct wterl_priv_data));
        enif_free(priv);
        return ENOMEM;
    }
    *priv_data = priv;

    char msg[1024];
    snprintf(msg, 1024, "NIF on_load complete (wterl version: %s, wiredtiger version: %s)", priv->wterl_vsn, priv->wiredtiger_vsn);
    __wterl_message_handler((WT_EVENT_HANDLER *)&priv->eh, msg);
    return 0;
}

static int
on_reload(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    UNUSED(env);
    UNUSED(priv_data);
    UNUSED(load_info);
    return 0; // TODO: implement
}

static void
on_unload(ErlNifEnv *env, void *priv_data)
{
    struct wterl_priv_data *priv = (struct wterl_priv_data *)priv_data;

    if (priv_data == NULL)
        return;

    DPRINTF("unloading wterl NIF (%p)", priv);
    ASYNC_NIF_UNLOAD(wterl, env, priv->async_nif_priv);

    /* At this point all WiredTiger state and threads are free'd/stopped so there
       is no chance that the event handler functions will be called so we can
       be sure that there won't be a race on eh.msg_env in the callback functions. */
    struct wterl_event_handlers *eh = &priv->eh;
    enif_mutex_destroy(eh->error_mutex);
    enif_mutex_destroy(eh->message_mutex);
    enif_mutex_destroy(eh->progress_mutex);
    if (eh->msg_env_message)
        enif_free_env(eh->msg_env_message);
    if (eh->msg_env_error)
        enif_free_env(eh->msg_env_error);
    if (eh->msg_env_progress)
        enif_free_env(eh->msg_env_progress);

    memset(priv, 0, sizeof(struct wterl_priv_data));
    enif_free(priv);

    priv_data = NULL;
}

static int
on_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info)
{
    UNUSED(priv_data);
    UNUSED(old_priv_data);
    UNUSED(load_info);
    ASYNC_NIF_UPGRADE(wterl, env); // TODO: implement
    return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"checkpoint_nif", 3, wterl_checkpoint},
    {"conn_close_nif", 2, wterl_conn_close},
    {"conn_open_nif", 4, wterl_conn_open},
    {"create_nif", 4, wterl_create},
    {"delete_nif", 4, wterl_delete},
    {"drop_nif", 4, wterl_drop},
    {"get_nif", 4, wterl_get},
    {"put_nif", 5, wterl_put},
    {"rename_nif", 5, wterl_rename},
    {"salvage_nif", 4, wterl_salvage},
    // TODO: {"txn_begin", 3, wterl_txn_begin},
    // TODO: {"txn_commit", 3, wterl_txn_commit},
    // TODO: {"txn_abort", 3, wterl_txn_abort},
    {"truncate_nif", 6, wterl_truncate},
    {"upgrade_nif", 4, wterl_upgrade},
    {"verify_nif", 4, wterl_verify},
    // TODO: {"cursor_get_key_nif", 2, wterl_cursor_get_key},
    // TODO: {"cursor_get_value_nif", 2, wterl_cursor_get_value},
    // TODO: {"cursor_get_nif", 2, wterl_cursor_get},
    // TODO: {"cursor_set_key_nif", 2, wterl_cursor_set_key},
    // TODO: {"cursor_set_value_nif", 2, wterl_cursor_set_value},
    // TODO: {"cursor_set_nif", 2, wterl_cursor_set},
    {"cursor_close_nif", 2, wterl_cursor_close},
    {"cursor_insert_nif", 4, wterl_cursor_insert},
    {"cursor_next_key_nif", 2, wterl_cursor_next_key},
    {"cursor_next_nif", 2, wterl_cursor_next},
    {"cursor_next_value_nif", 2, wterl_cursor_next_value},
    {"cursor_open_nif", 4, wterl_cursor_open},
    {"cursor_prev_key_nif", 2, wterl_cursor_prev_key},
    {"cursor_prev_nif", 2, wterl_cursor_prev},
    {"cursor_prev_value_nif", 2, wterl_cursor_prev_value},
    {"cursor_remove_nif", 3, wterl_cursor_remove},
    {"cursor_reset_nif", 2, wterl_cursor_reset},
    {"cursor_search_near_nif", 4, wterl_cursor_search_near},
    {"cursor_search_nif", 4, wterl_cursor_search},
    {"cursor_update_nif", 4, wterl_cursor_update},
    {"set_event_handler_pid", 1, wterl_set_event_handler_pid},
};

ERL_NIF_INIT(wterl, nif_funcs, &on_load, &on_reload, &on_upgrade, &on_unload);
