// -------------------------------------------------------------------
//
// wt: Erlang Wrapper for WiredTiger
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

static ErlNifResourceType* wt_conn_RESOURCE;
static ErlNifResourceType* wt_session_RESOURCE;
static ErlNifResourceType* wt_cursor_RESOURCE;

typedef struct {
    WT_CONNECTION* conn;
} WtConnHandle;

typedef struct {
    WT_SESSION* session;
} WtSessionHandle;

typedef struct {
    WT_CURSOR* cursor;
} WtCursorHandle;

typedef char Uri[128];					// object names

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;

typedef ERL_NIF_TERM (*CursorRetFun)(ErlNifEnv* env, WT_CURSOR* cursor, int rc);

// Prototypes
static ERL_NIF_TERM wt_conn_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_conn_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_insert(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_key_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc);
static ERL_NIF_TERM wt_cursor_kv_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc);
static ERL_NIF_TERM wt_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_next_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_next_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_np_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[],
                                           CursorRetFun cursor_ret_fun, int next);
static ERL_NIF_TERM wt_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_prev_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_prev_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_search(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_search_near(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_search_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int near);
static ERL_NIF_TERM wt_cursor_update(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_cursor_value_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc);
static ERL_NIF_TERM wt_session_checkpoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wt_session_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"conn_close", 1, wt_conn_close},
    {"conn_open", 2, wt_conn_open},
    {"cursor_close", 1, wt_cursor_close},
    {"cursor_insert", 3, wt_cursor_insert},
    {"cursor_next", 1, wt_cursor_next},
    {"cursor_next_key", 1, wt_cursor_next_key},
    {"cursor_next_value", 1, wt_cursor_next_value},
    {"cursor_open", 2, wt_cursor_open},
    {"cursor_prev", 1, wt_cursor_prev},
    {"cursor_prev_key", 1, wt_cursor_prev_key},
    {"cursor_prev_value", 1, wt_cursor_prev_value},
    {"cursor_remove", 3, wt_cursor_remove},
    {"cursor_reset", 1, wt_cursor_reset},
    {"cursor_search", 2, wt_cursor_search},
    {"cursor_search_near", 2, wt_cursor_search_near},
    {"cursor_update", 3, wt_cursor_update},
    {"session_checkpoint", 2, wt_session_checkpoint},
    {"session_close", 1, wt_session_close},
    {"session_create", 3, wt_session_create},
    {"session_delete", 3, wt_session_delete},
    {"session_drop", 3, wt_session_drop},
    {"session_get", 3, wt_session_get},
    {"session_open", 2, wt_session_open},
    {"session_put", 4, wt_session_put},
    {"session_rename", 4, wt_session_rename},
    {"session_salvage", 3, wt_session_salvage},
    {"session_truncate", 3, wt_session_truncate},
    {"session_upgrade", 3, wt_session_upgrade},
    {"session_verify", 3, wt_session_verify},
};

static inline ERL_NIF_TERM wt_strerror(ErlNifEnv* env, int rc)
{
    return rc == WT_NOTFOUND ?
        enif_make_atom(env, "not_found") :
        enif_make_tuple2(env, ATOM_ERROR,
                         enif_make_string(env, wiredtiger_strerror(rc), ERL_NIF_LATIN1));
}

static ERL_NIF_TERM wt_conn_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary config;
    char homedir[4096];
    if (enif_get_string(env, argv[0], homedir, sizeof homedir, ERL_NIF_LATIN1) &&
	enif_inspect_binary(env, argv[1], &config))
    {
        WT_CONNECTION* conn;
        int rc = wiredtiger_open(homedir, NULL, (const char*)config.data, &conn);
        if (rc == 0)
        {
            WtConnHandle* conn_handle = enif_alloc_resource(wt_conn_RESOURCE, sizeof(WtConnHandle));
            conn_handle->conn = conn;
            ERL_NIF_TERM result = enif_make_resource(env, conn_handle);
            enif_release_resource(conn_handle);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
	    return wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_conn_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtConnHandle* conn_handle;
    if (enif_get_resource(env, argv[0], wt_conn_RESOURCE, (void**)&conn_handle))
    {
        WT_CONNECTION* conn = conn_handle->conn;
        int rc = conn->close(conn, NULL);
        return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
    }
    return enif_make_badarg(env);
}

#define	WT_OP_CREATE		1
#define	WT_OP_DROP		2
#define	WT_OP_SALVAGE	3
#define	WT_OP_TRUNCATE	4
#define	WT_OP_UPGRADE	5
#define	WT_OP_VERIFY		6

static inline ERL_NIF_TERM wt_session_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int op)
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        int rc;
        Uri uri;
        ErlNifBinary config;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &config))
        {
            switch (op)
            {
            case WT_OP_CREATE:
                rc = session->create(session, uri, (const char*)config.data);
                break;
            case WT_OP_DROP:
                rc = session->drop(session, uri, (const char*)config.data);
                break;
            case WT_OP_SALVAGE:
                rc = session->salvage(session, uri, (const char*)config.data);
                break;
            case WT_OP_TRUNCATE:
                // Ignore the cursor start/stop form of truncation for now,
                // support only the full file truncation.
                rc = session->truncate(session, uri, NULL, NULL, (const char*)config.data);
                break;
            case WT_OP_UPGRADE:
                rc = session->upgrade(session, uri, (const char*)config.data);
                break;
            case WT_OP_VERIFY:
            default:
                rc = session->verify(session, uri, (const char*)config.data);
                break;
            }
            return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtConnHandle* conn_handle;
    ErlNifBinary config;
    if (enif_get_resource(env, argv[0], wt_conn_RESOURCE, (void**)&conn_handle) &&
	enif_inspect_binary(env, argv[1], &config))
    {
        WT_CONNECTION* conn = conn_handle->conn;
        WT_SESSION* session;
        int rc = conn->open_session(conn, NULL, (const char *)config.data, &session);
        if (rc == 0)
        {
            WtSessionHandle* session_handle =
                enif_alloc_resource(wt_session_RESOURCE, sizeof(WtSessionHandle));
            session_handle->session = session;
            ERL_NIF_TERM result = enif_make_resource(env, session_handle);
            enif_keep_resource(conn_handle);
            enif_release_resource(session_handle);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
	    return wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        int rc = session->close(session, NULL);
        return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_session_worker(env, argc, argv, WT_OP_CREATE);
}

static ERL_NIF_TERM wt_session_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_session_worker(env, argc, argv, WT_OP_DROP);
}

static ERL_NIF_TERM wt_session_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
        ErlNifBinary config;
        Uri oldname, newname;
        if (enif_get_string(env, argv[1], oldname, sizeof oldname, ERL_NIF_LATIN1) &&
            enif_get_string(env, argv[2], newname, sizeof newname, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[3], &config))
        {
            WT_SESSION* session = session_handle->session;
            int rc = session->rename(session, oldname, newname, (const char*)config.data);
            return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_session_worker(env, argc, argv, WT_OP_SALVAGE);
}

static ERL_NIF_TERM wt_session_checkpoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    ErlNifBinary config;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle) &&
        enif_inspect_binary(env, argv[1], &config))
    {
        WT_SESSION* session = session_handle->session;
        int rc = session->checkpoint(session, (const char*)config.data);
        return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_session_worker(env, argc, argv, WT_OP_TRUNCATE);
}

static ERL_NIF_TERM wt_session_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_session_worker(env, argc, argv, WT_OP_UPGRADE);
}

static ERL_NIF_TERM wt_session_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_session_worker(env, argc, argv, WT_OP_VERIFY);
}

static ERL_NIF_TERM wt_session_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
        Uri uri;
        ErlNifBinary key;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &key))
        {
            WT_SESSION* session = session_handle->session;
            WT_CURSOR* cursor;
            int rc = session->open_cursor(session, uri, NULL, "raw", &cursor);
            if (rc != 0)
            {
		return wt_strerror(env, rc);
            }
            WT_ITEM raw_key;
            raw_key.data = key.data;
            raw_key.size = key.size;
            cursor->set_key(cursor, &raw_key);
            rc = cursor->remove(cursor);
            cursor->close(cursor);
            return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
        Uri uri;
        ErlNifBinary key;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &key))
        {
            WT_SESSION* session = session_handle->session;
            WT_CURSOR* cursor;
            int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", &cursor);
            if (rc != 0)
            {
		return wt_strerror(env, rc);
            }
            WT_ITEM raw_key, raw_value;
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
                    return enif_make_tuple2(env, ATOM_OK, value);
                }
            }
            cursor->close(cursor);
            return wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_session_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
        Uri uri;
        ErlNifBinary key, value;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &key) &&
	    enif_inspect_binary(env, argv[3], &value))
        {
            WT_SESSION* session = session_handle->session;
            WT_CURSOR* cursor;
            int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", &cursor);
            if (rc != 0)
            {
		return wt_strerror(env, rc);
            }
            WT_ITEM raw_key, raw_value;
            raw_key.data = key.data;
            raw_key.size = key.size;
            cursor->set_key(cursor, &raw_key);
            raw_value.data = value.data;
            raw_value.size = value.size;
            cursor->set_value(cursor, &raw_value);
            rc = cursor->insert(cursor);
            cursor->close(cursor);
            return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtSessionHandle* session_handle;
    if (enif_get_resource(env, argv[0], wt_session_RESOURCE, (void**)&session_handle))
    {
	WT_CURSOR* cursor;
	Uri uri;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1))
        {
            WT_SESSION* session = session_handle->session;
	    int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", &cursor);
	    if (rc == 0)
            {
		WtCursorHandle* cursor_handle =
                    enif_alloc_resource(wt_cursor_RESOURCE, sizeof(WtCursorHandle));
		cursor_handle->cursor = cursor;
		ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
		enif_keep_resource(session_handle);
		enif_release_resource(cursor_handle);
		return enif_make_tuple2(env, ATOM_OK, result);
	    }
            else
            {
		return wt_strerror(env, rc);
	    }
	}
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], wt_cursor_RESOURCE, (void**)&cursor_handle))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
        int rc = cursor->close(cursor);
        return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_cursor_key_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
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
    return wt_strerror(env, rc);
}

static ERL_NIF_TERM wt_cursor_kv_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
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
    return wt_strerror(env, rc);
}

static ERL_NIF_TERM wt_cursor_value_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
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
    return wt_strerror(env, rc);
}

static ERL_NIF_TERM wt_cursor_np_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[],
                                           CursorRetFun cursor_ret, int prev)
{
    WtCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], wt_cursor_RESOURCE, (void**)&cursor_handle))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
	return cursor_ret(env, cursor, prev == 0 ? cursor->next(cursor) : cursor->prev(cursor));
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_np_worker(env, argc, argv, wt_cursor_kv_ret, 0);
}

static ERL_NIF_TERM wt_cursor_next_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_np_worker(env, argc, argv, wt_cursor_key_ret, 0);
}

static ERL_NIF_TERM wt_cursor_next_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_np_worker(env, argc, argv, wt_cursor_value_ret, 0);
}

static ERL_NIF_TERM wt_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_np_worker(env, argc, argv, wt_cursor_kv_ret, 1);
}

static ERL_NIF_TERM wt_cursor_prev_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_np_worker(env, argc, argv, wt_cursor_key_ret, 1);
}

static ERL_NIF_TERM wt_cursor_prev_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_np_worker(env, argc, argv, wt_cursor_value_ret, 1);
}

static ERL_NIF_TERM wt_cursor_search_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int near)
{
    WtCursorHandle *cursor_handle;
    ErlNifBinary key;
    if (enif_get_resource(env, argv[0], wt_cursor_RESOURCE, (void**)&cursor_handle) &&
	enif_inspect_binary(env, argv[1], &key))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
	WT_ITEM raw_key;
	int exact;
	raw_key.data = key.data;
	raw_key.size = key.size;
	cursor->set_key(cursor, &raw_key);

	// We currently ignore the less-than, greater-than or equals-to return information
	// from the cursor.search_near method.
	return wt_cursor_value_ret(env, cursor,
                                      near == 1 ?
                                      cursor->search_near(cursor, &exact) : cursor->search(cursor));
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_cursor_search(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_search_worker(env, argc, argv, 0);
}

static ERL_NIF_TERM wt_cursor_search_near(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_search_worker(env, argc, argv, 1);
}

static ERL_NIF_TERM wt_cursor_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    WtCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], wt_cursor_RESOURCE, (void**)&cursor_handle))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
        int rc = cursor->reset(cursor);
        return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
    }
    return enif_make_badarg(env);
}

#define	WT_OP_CURSOR_INSERT		1
#define	WT_OP_CURSOR_UPDATE		2
#define	WT_OP_CURSOR_REMOVE		3

static inline ERL_NIF_TERM wt_cursor_data_op(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int op)
{
    WtCursorHandle *cursor_handle;
    if (enif_get_resource(env, argv[0], wt_cursor_RESOURCE, (void**)&cursor_handle))
    {
        ErlNifBinary key, value;
	int rc;
        if (enif_inspect_binary(env, argv[1], &key) && enif_inspect_binary(env, argv[2], &value))
        {
            WT_CURSOR* cursor = cursor_handle->cursor;
	    WT_ITEM raw_key, raw_value;
	    raw_key.data = key.data;
	    raw_key.size = key.size;
	    cursor->set_key(cursor, &raw_key);
	    raw_value.data = value.data;
	    raw_value.size = value.size;
	    cursor->set_value(cursor, &raw_value);
	    switch (op)
            {
	    case WT_OP_CURSOR_INSERT:
		rc = cursor->insert(cursor);
		break;
	    case WT_OP_CURSOR_UPDATE:
		rc = cursor->update(cursor);
		break;
	    case WT_OP_CURSOR_REMOVE:
	    default:
		rc = cursor->remove(cursor);
		break;
	    }
	    return rc == 0 ? ATOM_OK : wt_strerror(env, rc);
	}
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wt_cursor_insert(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_data_op(env, argc, argv, WT_OP_CURSOR_INSERT);
}

static ERL_NIF_TERM wt_cursor_update(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_data_op(env, argc, argv, WT_OP_CURSOR_UPDATE);
}

static ERL_NIF_TERM wt_cursor_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wt_cursor_data_op(env, argc, argv, WT_OP_CURSOR_REMOVE);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    wt_conn_RESOURCE = enif_open_resource_type(env, NULL, "wt_conn_resource", NULL, flags, NULL);
    wt_session_RESOURCE = enif_open_resource_type(env, NULL, "wt_session_resource", NULL, flags, NULL);
    wt_cursor_RESOURCE = enif_open_resource_type(env, NULL, "wt_cursor_resource", NULL, flags, NULL);
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");
    return 0;
}

ERL_NIF_INIT(wt, nif_funcs, &on_load, NULL, NULL, NULL);
