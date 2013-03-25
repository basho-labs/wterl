// -------------------------------------------------------------------
//
// wterl: Erlang Wrapper for WiredTiger
//
// Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------
#include "erl_nif.h"
#include "erl_driver.h"

#include <stdio.h>
#include <string.h>

#include "wiredtiger.h"
#include "async_nif.h"

static ErlNifResourceType* wterl_conn_RESOURCE;
static ErlNifResourceType* wterl_session_RESOURCE;
static ErlNifResourceType* wterl_cursor_RESOURCE;

typedef struct {
    WT_CONNECTION* conn;
} WterlConnHandle;

typedef struct {
    WT_SESSION* session;
} WterlSessionHandle;

typedef struct {
    WT_CURSOR* cursor;
} WterlCursorHandle;

typedef char Uri[128];                                  // object names

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;


static inline ERL_NIF_TERM wterl_strerror(ErlNifEnv* env, int rc)
{
    return rc == WT_NOTFOUND ?
        enif_make_atom(env, "not_found") :
        enif_make_tuple2(env, ATOM_ERROR,
                         enif_make_string(env, wiredtiger_strerror(rc), ERL_NIF_LATIN1));
}

ASYNC_NIF_DECL(
  wterl_conn_open,
  { // struct

    ERL_NIF_TERM config;
    char homedir[4096];
  },
  { // pre

    if (!(argc == 2 &&
          enif_get_string(env, argv[0], args->homedir, sizeof args->homedir, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[1]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
  },
  { // work

    WT_CONNECTION* conn;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }

    int rc = wiredtiger_open(args->homedir, NULL, (const char*)config.data, &conn);
    if (rc == 0)
    {
      WterlConnHandle* conn_handle = enif_alloc_resource(wterl_conn_RESOURCE, sizeof(WterlConnHandle));
      conn_handle->conn = conn;
      ERL_NIF_TERM result = enif_make_resource(env, conn_handle);
      enif_release_resource(conn_handle);
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
    }
    else
    {
      ASYNC_NIF_REPLY(wterl_strerror(env, rc));
    }
  },
  { // post

  });

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

    WT_CONNECTION* conn = args->conn_handle->conn;
    int rc = conn->close(conn, NULL);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_open,
  { // struct

    ERL_NIF_TERM config;
    WterlConnHandle* conn_handle;
  },
  { // pre

    if (!(argc == 2 &&
          enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&args->conn_handle)  &&
          enif_is_binary(env, argv[1]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    enif_keep_resource((void*)args->conn_handle);
  },
  { // work

    WT_CONNECTION* conn = args->conn_handle->conn;
    WT_SESSION* session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = conn->open_session(conn, NULL, (const char*)config.data, &session);
    if (rc == 0)
    {
      WterlSessionHandle* session_handle =
        enif_alloc_resource(wterl_session_RESOURCE, sizeof(WterlSessionHandle));
      session_handle->session = session;
      ERL_NIF_TERM result = enif_make_resource(env, session_handle);
      enif_keep_resource(args->conn_handle);
      enif_release_resource(session_handle);
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
    }
    else
    {
      ASYNC_NIF_REPLY(wterl_strerror(env, rc));
    }
  },
  { // post

    enif_release_resource((void*)args->conn_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_close,
  { // struct

    WterlSessionHandle* session_handle;
  },
  { // pre

    if (!(argc == 1 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    int rc = session->close(session, NULL);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_create,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->create(session, args->uri, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_drop,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->drop(session, args->uri, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_rename,
  { // struct

    WterlSessionHandle* session_handle;
    ERL_NIF_TERM config;
    Uri oldname;
    Uri newname;
  },
  { // pre

    if (!(argc == 4 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->oldname, sizeof args->oldname, ERL_NIF_LATIN1) &&
          enif_get_string(env, argv[2], args->newname, sizeof args->newname, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[3]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->rename(session, args->oldname, args->newname, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_salvage,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->salvage(session, args->uri, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_checkpoint,
  { // struct

    WterlSessionHandle* session_handle;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 2 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_is_binary(env, argv[1]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[1]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->checkpoint(session, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_truncate,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    // Ignore the cursor start/stop form of truncation for now,
    // support only the full file truncation.
    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->truncate(session, args->uri, NULL, NULL, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_upgrade,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->upgrade(session, args->uri, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_verify,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM config;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->config = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary config;
    if (!enif_inspect_binary(env, args->config, &config)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    int rc = session->verify(session, args->uri, (const char*)config.data);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_delete,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM key;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_CURSOR* cursor;
    int rc = session->open_cursor(session, args->uri, NULL, "raw", &cursor);
    if (rc != 0)
    {
      ASYNC_NIF_REPLY(wterl_strerror(env, rc));
      return;
    }
    WT_ITEM raw_key;
    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);
    rc = cursor->remove(cursor);
    cursor->close(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_get,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM key;
  },
  { // pre

    if (!(argc == 3 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    ErlNifBinary key;
    if (!enif_inspect_binary(env, args->key, &key)) {
      ASYNC_NIF_REPLY(enif_make_badarg(env));
      return;
    }
    WT_CURSOR* cursor;
    int rc = session->open_cursor(session, args->uri, NULL, "overwrite,raw", &cursor);
    if (rc != 0)
    {
      ASYNC_NIF_REPLY(wterl_strerror(env, rc));
      return;
    }
    WT_ITEM raw_key;
    WT_ITEM raw_value;
    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);
    rc = cursor->search(cursor);
    if (rc == 0)
    {
      rc = cursor->get_value(cursor, &raw_value);
      if (rc == 0)
      {
        ERL_NIF_TERM value;
        unsigned char* bin = enif_make_new_binary(env, raw_value.size, &value);
        memcpy(bin, raw_value.data, raw_value.size);
        cursor->close(cursor);
        ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, value));
        return;
      }
    }
    cursor->close(cursor);
    ASYNC_NIF_REPLY(wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_session_put,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
    ERL_NIF_TERM key;
    ERL_NIF_TERM value;
  },
  { // pre

    if (!(argc == 4 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1) &&
          enif_is_binary(env, argv[2]) &&
          enif_is_binary(env, argv[3]))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    args->key = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[2]);
    args->value = enif_make_copy(ASYNC_NIF_WORK_ENV, argv[3]);
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
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
    WT_CURSOR* cursor;
    int rc = session->open_cursor(session, args->uri, NULL, "overwrite,raw", &cursor);
    if (rc != 0)
    {
      ASYNC_NIF_REPLY(wterl_strerror(env, rc));
      return;
    }
    WT_ITEM raw_key;
    WT_ITEM raw_value;
    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);
    raw_value.data = value.data;
    raw_value.size = value.size;
    cursor->set_value(cursor, &raw_value);
    rc = cursor->insert(cursor);
    cursor->close(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

ASYNC_NIF_DECL(
  wterl_cursor_open,
  { // struct

    WterlSessionHandle* session_handle;
    Uri uri;
  },
  { // pre

    if (!(argc == 2 &&
          enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&args->session_handle) &&
          enif_get_string(env, argv[1], args->uri, sizeof args->uri, ERL_NIF_LATIN1))) {
      ASYNC_NIF_RETURN_BADARG();
    }
    enif_keep_resource((void*)args->session_handle);
  },
  { // work

    WT_SESSION* session = args->session_handle->session;
    WT_CURSOR* cursor;
    int rc = session->open_cursor(session, args->uri, NULL, "overwrite,raw", &cursor);
    if (rc == 0)
    {
      WterlCursorHandle* cursor_handle =
        enif_alloc_resource(wterl_cursor_RESOURCE, sizeof(WterlCursorHandle));
      cursor_handle->cursor = cursor;
      ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
      enif_keep_resource(args->session_handle);
      enif_release_resource(cursor_handle);
      ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
    }
    else
    {
      ASYNC_NIF_REPLY(wterl_strerror(env, rc));
    }
  },
  { // post

    enif_release_resource((void*)args->session_handle);
  });

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

    WT_CURSOR* cursor = args->cursor_handle->cursor;
    int rc = cursor->close(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

static ERL_NIF_TERM wterl_cursor_key_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0)
    {
        WT_ITEM raw_key;
        rc = cursor->get_key(cursor, &raw_key);
        if (rc == 0)
        {
            ERL_NIF_TERM key;
            memcpy(enif_make_new_binary(env, raw_key.size, &key), raw_key.data, raw_key.size);
            return enif_make_tuple2(env, ATOM_OK, key);
        }
    }
    return wterl_strerror(env, rc);
}

static ERL_NIF_TERM wterl_cursor_kv_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0)
    {
        WT_ITEM raw_key, raw_value;
        rc = cursor->get_key(cursor, &raw_key);
        if (rc == 0)
        {
            rc = cursor->get_value(cursor, &raw_value);
            if (rc == 0)
            {
                ERL_NIF_TERM key, value;
                memcpy(enif_make_new_binary(env, raw_key.size, &key), raw_key.data, raw_key.size);
                memcpy(enif_make_new_binary(env, raw_value.size, &value), raw_value.data, raw_value.size);
                return enif_make_tuple3(env, ATOM_OK, key, value);
            }
        }
    }
    return wterl_strerror(env, rc);
}

static ERL_NIF_TERM wterl_cursor_value_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0)
    {
        WT_ITEM raw_value;
        rc = cursor->get_value(cursor, &raw_value);
        if (rc == 0)
        {
            ERL_NIF_TERM value;
            memcpy(enif_make_new_binary(env, raw_value.size, &value), raw_value.data, raw_value.size);
            return enif_make_tuple2(env, ATOM_OK, value);
        }
    }
    return wterl_strerror(env, rc);
}

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
    ASYNC_NIF_REPLY(wterl_cursor_kv_ret(env, cursor, cursor->next(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ASYNC_NIF_REPLY(wterl_cursor_key_ret(env, cursor, cursor->next(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ASYNC_NIF_REPLY(wterl_cursor_value_ret(env, cursor, cursor->next(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ASYNC_NIF_REPLY(wterl_cursor_kv_ret(env, cursor, cursor->prev(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ASYNC_NIF_REPLY(wterl_cursor_key_ret(env, cursor, cursor->prev(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ASYNC_NIF_REPLY(wterl_cursor_value_ret(env, cursor, cursor->prev(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    WT_ITEM raw_key;

    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);

    // We currently ignore the less-than, greater-than or equals-to return information
    // from the cursor.search_near method.
    ASYNC_NIF_REPLY(wterl_cursor_value_ret(env, cursor, cursor->search(cursor)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    WT_ITEM raw_key;
    int exact;

    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);

    // We currently ignore the less-than, greater-than or equals-to return information
    // from the cursor.search_near method.
    ASYNC_NIF_REPLY(wterl_cursor_value_ret(env, cursor, cursor->search_near(cursor, &exact)));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    WT_ITEM raw_key;
    WT_ITEM raw_value;

    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);
    raw_value.data = value.data;
    raw_value.size = value.size;
    cursor->set_value(cursor, &raw_value);
    int rc = cursor->insert(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    WT_ITEM raw_key;
    WT_ITEM raw_value;

    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);
    raw_value.data = value.data;
    raw_value.size = value.size;
    cursor->set_value(cursor, &raw_value);
    int rc = cursor->update(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

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
    WT_ITEM raw_key;

    raw_key.data = key.data;
    raw_key.size = key.size;
    cursor->set_key(cursor, &raw_key);
    int rc = cursor->remove(cursor);
    ASYNC_NIF_REPLY(rc == 0 ? ATOM_OK : wterl_strerror(env, rc));
  },
  { // post

    enif_release_resource((void*)args->cursor_handle);
  });

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    wterl_conn_RESOURCE = enif_open_resource_type(env, NULL, "wterl_conn_resource", NULL, flags, NULL);
    wterl_session_RESOURCE = enif_open_resource_type(env, NULL, "wterl_session_resource", NULL, flags, NULL);
    wterl_cursor_RESOURCE = enif_open_resource_type(env, NULL, "wterl_cursor_resource", NULL, flags, NULL);
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");

    ASYNC_NIF_LOAD();

    return 0;
}

static void on_unload(ErlNifEnv* env, void* priv_data)
{
  ASYNC_NIF_UNLOAD();
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
  ASYNC_NIF_UPGRADE();
  return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"conn_open_nif", 3, wterl_conn_open},
    {"conn_close_nif", 2, wterl_conn_close},
    {"session_open_nif", 3, wterl_session_open},
    {"session_close_nif", 2, wterl_session_close},
    {"session_create_nif", 4, wterl_session_create},
    {"session_drop_nif", 4, wterl_session_drop},
    {"session_rename_nif", 5, wterl_session_rename},
    {"session_salvage_nif", 4, wterl_session_salvage},
    {"session_checkpoint_nif", 3, wterl_session_checkpoint},
    {"session_truncate_nif", 4, wterl_session_truncate},
    {"session_upgrade_nif", 4, wterl_session_upgrade},
    {"session_verify_nif", 4, wterl_session_verify},
    {"session_delete_nif", 4, wterl_session_delete},
    {"session_get_nif", 4, wterl_session_get},
    {"session_put_nif", 5, wterl_session_put},
    {"cursor_open_nif", 3, wterl_cursor_open},
    {"cursor_close_nif", 2, wterl_cursor_close},
    {"cursor_next_nif", 2, wterl_cursor_next},
    {"cursor_next_key_nif", 2, wterl_cursor_next_key},
    {"cursor_next_value_nif", 2, wterl_cursor_next_value},
    {"cursor_prev_nif", 2, wterl_cursor_prev},
    {"cursor_prev_key_nif", 2, wterl_cursor_prev_key},
    {"cursor_prev_value_nif", 2, wterl_cursor_prev_value},
    {"cursor_search_nif", 3, wterl_cursor_search},
    {"cursor_search_near_nif", 3, wterl_cursor_search_near},
    {"cursor_reset_nif", 2, wterl_cursor_reset},
    {"cursor_insert_nif", 4, wterl_cursor_insert},
    {"cursor_update_nif", 4, wterl_cursor_update},
    {"cursor_remove_nif", 3, wterl_cursor_remove},
};

ERL_NIF_INIT(wterl, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload);
