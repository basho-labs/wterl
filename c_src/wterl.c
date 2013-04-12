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
#include <errno.h>

#include "wiredtiger.h"
#include "async_nif.h"
#include "khash.h"

#ifdef DEBUG
#include <stdio.h>
#include <stdarg.h>
void debugf(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\r\n");
    fflush(stderr);
    va_end(ap);
}
#else
#  define debugf(X, ...) {}
#endif

static ErlNifResourceType *wterl_conn_RESOURCE;
static ErlNifResourceType *wterl_cursor_RESOURCE;

KHASH_MAP_INIT_STR(cursors, WT_CURSOR*);

/**
 * We will have exactly one (1) WterlCtx for each async worker thread.  As
 * requests arrive we will reuse the same WterlConnHandle->contexts[worker_id]
 * WterlCtx in the work block ensuring that each async worker thread a) has
 * a separate WT_SESSION (because they are not thread safe) and b) when
 * possible we avoid opening new cursors by first looking for one in the
 * cursors hash table.  In practice this means we could have (num_workers
 * * num_tables) of cursors open which we need to account for when setting
 * session_max in the configuration of WiredTiger so that it creates enough
 * hazard pointers for this extreme case.
 *
 * Note: We don't protect access to this struct with a mutex because it will
 * only be accessed by the same worker thread.
 */
typedef struct {
    WT_SESSION *session;
    khash_t(cursors) *cursors;
} WterlCtx;

typedef struct {
    WT_CONNECTION *conn;
    const char *session_config;
    ErlNifMutex *context_mutex;
    unsigned int num_contexts;
    WterlCtx contexts[ASYNC_NIF_MAX_WORKERS];
} WterlConnHandle;

typedef struct {
    WT_CURSOR *cursor;
    WT_SESSION *session;
} WterlCursorHandle;

/* WiredTiger object names*/
typedef char Uri[128];

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_FIRST;
static ERL_NIF_TERM ATOM_LAST;

ASYNC_NIF_INIT(wterl);

/**
 * Get the per-worker reusable WT_SESSION for a worker_id.
 */
static int
__session_for(WterlConnHandle *conn_handle, unsigned int worker_id, WT_SESSION **session)
{
    WterlCtx *ctx = &conn_handle->contexts[worker_id];
    *session = ctx->session;
    if (*session == NULL) {
	/* Create a context for this worker thread to reuse. */
        enif_mutex_lock(conn_handle->context_mutex);
	WT_CONNECTION *conn = conn_handle->conn;
	int rc = conn->open_session(conn, NULL, conn_handle->session_config, session);
	if (rc != 0) {
            enif_mutex_unlock(conn_handle->context_mutex);
	    return rc;
        }
	ctx->session = *session;
	ctx->cursors = kh_init(cursors);
        conn_handle->num_contexts++;
        enif_mutex_unlock(conn_handle->context_mutex);
    }
    return 0;
}

/**
 * Close all sessions and all cursors open on any objects.
 *
 * Note: always call within enif_mutex_lock/unlock(conn_handle->context_mutex)
 */
void
__close_all_sessions(WterlConnHandle *conn_handle)
{
    int i;
    for (i = 0; i < ASYNC_NIF_MAX_WORKERS; i++) {
        WterlCtx *ctx = &conn_handle->contexts[i];
        if (ctx->session) {
            WT_SESSION *session = ctx->session;
            khash_t(cursors) *h = ctx->cursors;
            khiter_t itr;
            for (itr = kh_begin(h); itr != kh_end(h); ++itr) {
                if (kh_exist(h, itr)) {
                    // WT_CURSOR *cursor = kh_val(h, itr);
                    // cursor->close(cursor);
                    char *key = (char *)kh_key(h, itr);
                    enif_free(key);
                }
            }
            kh_destroy(cursors, h);
            session->close(session, NULL);
            bzero(&conn_handle->contexts[i], sizeof(WterlCtx));
        }
    }
    conn_handle->num_contexts = 0;
}

/**
 * Close cursors open on 'uri' object.
 *
 * Note: always call within enif_mutex_lock/unlock(conn_handle->context_mutex)
 */
void
__close_cursors_on(WterlConnHandle *conn_handle, const char *uri) // TODO: race?
{
    int i;
    for (i = 0; i < ASYNC_NIF_MAX_WORKERS; i++) {
        WterlCtx *ctx = &conn_handle->contexts[i];
        if (ctx->session) {
            khash_t(cursors) *h = ctx->cursors;
            khiter_t itr = kh_get(cursors, h, (char *)uri);
            if (itr != kh_end(h)) {
                WT_CURSOR *cursor = kh_value(h, itr);
                char *key = (char *)kh_key(h, itr);
                cursor->close(cursor);
                kh_del(cursors, h, itr);
                enif_free(key);
            }
        }
    }
}

/**
 * Get a reusable cursor that was opened for a particular worker within its
 * session.
 */
static int
__retain_cursor(WterlConnHandle *conn_handle, unsigned int worker_id, const char *uri, WT_CURSOR **cursor)
{
    /* Check to see if we have a cursor open for this uri and if so reuse it. */
    WterlCtx *ctx = &conn_handle->contexts[worker_id];
    khash_t(cursors) *h = ctx->cursors;
    khiter_t itr = kh_get(cursors, h, (char *)uri);
    if (itr != kh_end(h)) {
	// key exists in hash table, retrieve it
	*cursor = (WT_CURSOR*)kh_value(h, itr);
    } else {
	// key does not exist in hash table, create and insert one
        enif_mutex_lock(conn_handle->context_mutex);
	WT_SESSION *session = conn_handle->contexts[worker_id].session;
	int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", cursor);
	if (rc != 0) {
            enif_mutex_unlock(conn_handle->context_mutex);
	    return rc;
        }

        char *key = enif_alloc(sizeof(Uri));
        if (!key) {
            session->close(session, NULL);
            enif_mutex_unlock(conn_handle->context_mutex);
	    return ENOMEM;
        }
        memcpy(key, uri, 128);

        int itr_status;
        itr = kh_put(cursors, h, key, &itr_status);
	kh_value(h, itr) = *cursor;
        enif_mutex_unlock(conn_handle->context_mutex);
    }
    return 0;
}

static void
__release_cursor(WterlConnHandle *conn_handle, unsigned int worker_id, const char *uri, WT_CURSOR *cursor)
{
    cursor->reset(cursor);
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
        /* TODO: The string for the error message provided by strerror() for
           any given errno value may be different across platforms, return
           {atom, string} and may have been localized too. */
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
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_string(env, argv[0], args->homedir, sizeof args->homedir, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[1]) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    args->session_config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
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
    //debugf("c: %d // %s\ns: %d // %s", config.size, (char *)config.data, (char *)session_config.data, session_config.size);
    int rc = wiredtiger_open(args->homedir, NULL, config.data[0] != 0 ? (const char*)config.data : NULL, &conn);
    if (rc == 0) {
      WterlConnHandle *conn_handle = enif_alloc_resource(wterl_conn_RESOURCE, sizeof(WterlConnHandle));
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
      conn_handle->conn = conn;
      conn_handle->num_contexts = 0;
      bzero(conn_handle->contexts, sizeof(WterlCtx) * ASYNC_NIF_MAX_WORKERS);
      conn_handle->context_mutex = enif_mutex_create(NULL);
      ERL_NIF_TERM result = enif_make_resource(env, conn_handle);
      enif_release_resource(conn_handle);
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
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* Free up the shared sessions and cursors. */
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_all_sessions(args->conn_handle);
    if (args->conn_handle->session_config) {
        enif_free((char *)args->conn_handle->session_config);
        args->conn_handle->session_config = NULL;
    }
    WT_CONNECTION* conn = args->conn_handle->conn;
    int rc = conn->close(conn, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
    enif_mutex_destroy(args->conn_handle->context_mutex);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      enif_mutex_unlock(args->conn_handle->context_mutex);
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
        enif_mutex_unlock(args->conn_handle->context_mutex);
	return;
    }
    /* Note: we must first close all cursors referencing this object or this
       operation will fail with EBUSY(16) "Device or resource busy". */

    // TODO: add a condition for this object name, test for that in cursor
    // create, pause the async nif layer's workers, find and close open cursors
    // on this table, restart worker threads, do the drop, remove the condition
    // variable (read: punt for now, expect a lot of EBUSYs)

    rc = session->drop(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
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
          enif_get_string(env, argv[1], args->oldname, sizeof args->oldname, ERL_NIF_LATIN1) &&
          enif_get_string(env, argv[2], args->newname, sizeof args->newname, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[3]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_cursors_on(args->conn_handle, args->oldname);

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

    /* Note: we must first close all cursors referencing this object or this
       operation will fail with EBUSY(16) "Device or resource busy". */
    // TODO: see drop's note, same goes here.
    rc = session->rename(session, args->oldname, args->newname, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

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

    rc = session->salvage(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
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
 * argv[1]    object name URI string
 * argv[2]    config string as an Erlang binary
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
    WT_SESSION *session = NULL;
    int rc = __session_for(args->conn_handle, worker_id, &session);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }
    rc = session->checkpoint(session, (const char*)config.data);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
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
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      enif_mutex_unlock(args->conn_handle->context_mutex);
      return;
    }

    /* We create, use and discard a WT_SESSION and up to two WT_CURSORS here because
       a) we'll have to close out other, shared cursors on this table first and b) we
       don't anticipate doing this operation frequently enough to impact performance. */
    // TODO: see drop's note, same goes here.
    WT_CONNECTION *conn = args->conn_handle->conn;
    WT_SESSION *session = NULL;
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
        enif_mutex_unlock(args->conn_handle->context_mutex);
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
            ASYNC_NIF_REPLY(enif_make_badarg(env));
            enif_mutex_unlock(args->conn_handle->context_mutex);
            return;
        }
    }
    rc = session->open_cursor(session, args->uri, NULL, "raw", &start);
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        session->close(session, NULL);
        enif_mutex_unlock(args->conn_handle->context_mutex);
        return;
    }
    /* Position the start cursor at the first record or the specified record. */
    if (args->from_first) {
        rc = start->next(start);
        if (rc != 0) {
            ASYNC_NIF_REPLY(__strerror_term(env, rc));
            start->close(start);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->context_mutex);
            return;
        }
    } else {
	WT_ITEM item_start;
	item_start.data = start_key.data;
	item_start.size = start_key.size;
	start->set_key(start, item_start);
        rc = start->search(start);
        if (rc != 0) {
            ASYNC_NIF_REPLY(__strerror_term(env, rc));
            start->close(start);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->context_mutex);
            return;
        }
    }

    if (!args->to_last) {
        if (!enif_inspect_binary(env, args->stop, &stop_key)) {
            ASYNC_NIF_REPLY(enif_make_badarg(env));
            enif_mutex_unlock(args->conn_handle->context_mutex);
            return;
        }
    }
    rc = session->open_cursor(session, args->uri, NULL, "raw", &stop);
    if (rc != 0) {
        ASYNC_NIF_REPLY(__strerror_term(env, rc));
        start->close(start);
        session->close(session, NULL);
        enif_mutex_unlock(args->conn_handle->context_mutex);
        return;
    }
    /* Position the stop cursor at the last record or the specified record. */
    if (args->to_last) {
        rc = stop->prev(stop);
        if (rc != 0) {
            ASYNC_NIF_REPLY(__strerror_term(env, rc));
            start->close(start);
            stop->close(stop);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->context_mutex);
            return;
        }
    } else {
	WT_ITEM item_stop;
	item_stop.data = stop_key.data;
	item_stop.size = stop_key.size;
	stop->set_key(stop, item_stop);
        rc = stop->search(stop);
        if (rc != 0) {
            ASYNC_NIF_REPLY(__strerror_term(env, rc));
            start->close(start);
            stop->close(stop);
            session->close(session, NULL);
            enif_mutex_unlock(args->conn_handle->context_mutex);
            return;
        }
    }

    /* Always pass NULL for URI here because we always specify the range with the
       start and stop cursors which were opened referencing that URI. */
    rc = session->truncate(session, NULL, start, stop, (const char*)config.data);

    if (start) start->close(start);
    if (stop) stop->close(stop);
    if (session) session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_cursors_on(args->conn_handle, args->uri);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      enif_mutex_unlock(args->conn_handle->context_mutex);
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
        enif_mutex_unlock(args->conn_handle->context_mutex);
	return;
    }

    rc = session->upgrade(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    /* This call requires that there be no open cursors referencing the object. */
    enif_mutex_lock(args->conn_handle->context_mutex);
    __close_all_sessions(args->conn_handle);

    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      enif_mutex_unlock(args->conn_handle->context_mutex);
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
        enif_mutex_unlock(args->conn_handle->context_mutex);
	return;
    }

    rc = session->verify(session, args->uri, (const char*)config.data);
    (void)session->close(session, NULL);
    enif_mutex_unlock(args->conn_handle->context_mutex);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    WT_SESSION *session = NULL;
    int rc = __session_for(args->conn_handle, worker_id, &session);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    WT_CURSOR *cursor = NULL;
    rc = __retain_cursor(args->conn_handle, worker_id, args->uri, &cursor);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    WT_ITEM item_key;
    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    rc = cursor->remove(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
    __release_cursor(args->conn_handle, worker_id, args->uri, cursor);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    WT_SESSION *session = NULL;
    int rc = __session_for(args->conn_handle, worker_id, &session);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    WT_CURSOR *cursor = NULL;
    rc = __retain_cursor(args->conn_handle, worker_id, args->uri, &cursor);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    WT_ITEM item_key;
    WT_ITEM item_value;
    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);
    rc = cursor->search(cursor);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    rc = cursor->get_value(cursor, &item_value);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }
    ERL_NIF_TERM value;
    unsigned char *bin = enif_make_new_binary(env, item_value.size, &value);
    memcpy(bin, item_value.data, item_value.size);
    ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, value));
    __release_cursor(args->conn_handle, worker_id, args->uri, cursor);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]) &&
          enif_is_binary(env, argv[3]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    args->value = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    enif_keep_resource((void*)args->conn_handle);
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

    WT_SESSION *session = NULL;
    int rc = __session_for(args->conn_handle, worker_id, &session);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    WT_CURSOR *cursor = NULL;
    rc = __retain_cursor(args->conn_handle, worker_id, args->uri, &cursor);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
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
    rc = cursor->insert(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
    __release_cursor(args->conn_handle, worker_id, args->uri, cursor);
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
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
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
    //debugf("cursor open: %s", (char *)args->conn_handle->session_config);
    int rc = conn->open_session(conn, NULL, args->conn_handle->session_config, &session);
    if (rc != 0) {
	ASYNC_NIF_REPLY(__strerror_term(env, rc));
	return;
    }

    WT_CURSOR* cursor;
    rc = session->open_cursor(session, args->uri, NULL, (config.data[0] != 0) ? (char *)config.data : "overwrite,raw", &cursor);
    if (rc != 0) {
      ASYNC_NIF_REPLY(__strerror_term(env, rc));
      return;
    }

    WterlCursorHandle* cursor_handle = enif_alloc_resource(wterl_cursor_RESOURCE, sizeof(WterlCursorHandle));
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

    WT_CURSOR *cursor = args->cursor_handle->cursor;
    WT_SESSION *session = args->cursor_handle->session;
    /* Note: session->close() will cause all open cursors in the session to be
       closed first, so we don't have explicitly to do that here. */
    int rc = cursor->close(cursor);
    (void)session->close(session, NULL);
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
 */
ASYNC_NIF_DECL(
  wterl_cursor_search,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
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

    ASYNC_NIF_REPLY(__cursor_value_ret(env, cursor, cursor->search(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

/**
 * Position the cursor at the record matching the key if it exists, or a record
 * that would be adjacent.
 *
 * argv[0]    WterlCursorHandle resource
 */
ASYNC_NIF_DECL(
  wterl_cursor_search_near,
  { // struct

    WterlCursorHandle *cursor_handle;
    ERL_NIF_TERM key;
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
    int exact;

    item_key.data = key.data;
    item_key.size = key.size;
    cursor->set_key(cursor, &item_key);

    // TODO: We currently ignore the less-than, greater-than or equals-to
    // return information from the cursor.search_near method.
    ASYNC_NIF_REPLY(__cursor_value_ret(env, cursor, cursor->search_near(cursor, &exact)));
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
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : __strerror_term(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    wterl_conn_RESOURCE = enif_open_resource_type(env, NULL, "wterl_conn_resource",
						  NULL, flags, NULL);
    wterl_cursor_RESOURCE = enif_open_resource_type(env, NULL, "wterl_cursor_resource",
						    NULL, flags, NULL);

    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
    ATOM_FIRST = enif_make_atom(env, "first");
    ATOM_LAST = enif_make_atom(env, "last");

    ASYNC_NIF_LOAD(wterl, *priv_data);

    return *priv_data ? 0 : -1;
}

static int
on_reload(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    return 0; // TODO: determine what should be done here, if anything...
}

static void
on_unload(ErlNifEnv *env, void *priv_data)
{
    ASYNC_NIF_UNLOAD(wterl, env);
}

static int
on_upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info)
{
    ASYNC_NIF_UPGRADE(wterl, env);
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
    {"cursor_search_near_nif", 3, wterl_cursor_search_near},
    {"cursor_search_nif", 3, wterl_cursor_search},
    {"cursor_update_nif", 4, wterl_cursor_update},
};

ERL_NIF_INIT(wterl, nif_funcs, &on_load, &on_reload, &on_upgrade, &on_unload);
