// -------------------------------------------------------------------
//
// wterl: Erlang Wrapper for WiredTiger
//
// Copyright (c) 2011 Basho Technologies, Inc. All Rights Reserved.
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

static ErlNifResourceType* wterl_conn_RESOURCE;
static ErlNifResourceType* wterl_session_RESOURCE;
static ErlNifResourceType* wterl_cursor_RESOURCE;

typedef struct
{
    WT_CONNECTION* conn;
} wterl_conn_handle;

typedef struct
{
    WT_SESSION* session;
} wterl_session_handle;

typedef struct
{
    WT_CURSOR* cursor;
} wterl_cursor_handle;

typedef char Uri[128];					// object names

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;

// Prototypes
static ERL_NIF_TERM wterl_conn_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_conn_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_insert(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_np_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int next);
static ERL_NIF_TERM wterl_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc);
static ERL_NIF_TERM wterl_cursor_search(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_search_near(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_cursor_search_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int near);
static ERL_NIF_TERM wterl_cursor_update(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"conn_close", 1, wterl_conn_close},
    {"conn_open", 2, wterl_conn_open},
    {"cursor_close", 1, wterl_cursor_close},
    {"cursor_insert", 3, wterl_cursor_insert},
    {"cursor_next", 1, wterl_cursor_next},
    {"cursor_open", 2, wterl_cursor_open},
    {"cursor_prev", 1, wterl_cursor_prev},
    {"cursor_remove", 3, wterl_cursor_remove},
    {"cursor_reset", 1, wterl_cursor_reset},
    {"cursor_search", 2, wterl_cursor_search},
    {"cursor_search_near", 2, wterl_cursor_search_near},
    {"cursor_update", 3, wterl_cursor_update},
    {"session_close", 1, wterl_session_close},
    {"session_create", 3, wterl_session_create},
    {"session_delete", 3, wterl_session_delete},
    {"session_drop", 3, wterl_session_drop},
    {"session_get", 3, wterl_session_get},
    {"session_open", 1, wterl_session_open},
    {"session_put", 4, wterl_session_put},
    {"session_rename", 4, wterl_session_rename},
    {"session_salvage", 3, wterl_session_salvage},
    {"session_sync", 3, wterl_session_sync},
    {"session_truncate", 3, wterl_session_truncate},
    {"session_upgrade", 3, wterl_session_upgrade},
    {"session_verify", 3, wterl_session_verify},
};

static inline ERL_NIF_TERM wterl_strerror(ErlNifEnv* env, int rc)
{
    if (rc == WT_NOTFOUND)
	return enif_make_atom(env, "not_found");
    else
	return enif_make_tuple2(env, ATOM_ERROR,
			        enif_make_string(env, wiredtiger_strerror(rc), ERL_NIF_LATIN1));
}

static ERL_NIF_TERM wterl_conn_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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
            wterl_conn_handle* conn_handle =
		enif_alloc_resource(wterl_conn_RESOURCE, sizeof(wterl_conn_handle));
            conn_handle->conn = conn;
            ERL_NIF_TERM result = enif_make_resource(env, conn_handle);
            enif_release_resource(conn_handle);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
	    return wterl_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_conn_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_conn_handle* conn_handle;
    if (enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&conn_handle))
    {
        WT_CONNECTION* conn = conn_handle->conn;
        int rc = conn->close(conn, NULL);
        if (rc != 0)
        {
	    return wterl_strerror(env, rc);
        }
	else {
		return ATOM_OK;
	}
    }
    return enif_make_badarg(env);
}

#define	WTERL_OP_CREATE		1
#define	WTERL_OP_DROP		2
#define	WTERL_OP_SALVAGE	3
#define	WTERL_OP_SYNC		4
#define	WTERL_OP_TRUNCATE	5
#define	WTERL_OP_UPGRADE	6
#define	WTERL_OP_VERIFY		7

static inline ERL_NIF_TERM wterl_session_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int op)
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
	ErlNifBinary config;
        Uri uri;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &config))
        {
	    int rc;
	    switch (op) {
	    case WTERL_OP_CREATE:
              rc = session->create(session, uri, (const char*)config.data);
		break;
	    case WTERL_OP_DROP:
              rc = session->drop(session, uri, (const char*)config.data);
		break;
	    case WTERL_OP_SALVAGE:
              rc = session->salvage(session, uri, (const char*)config.data);
		break;
	    case WTERL_OP_SYNC:
              rc = session->sync(session, uri, (const char*)config.data);
		break;
	    case WTERL_OP_TRUNCATE:
		// Ignore the cursor start/stop form of truncation for now,
		// support only the full file truncation.
              rc = session->truncate(session, uri, NULL, NULL, (const char*)config.data);
		break;
	    case WTERL_OP_UPGRADE:
              rc = session->upgrade(session, uri, (const char*)config.data);
		break;
	    default:
	    case WTERL_OP_VERIFY:
              rc = session->verify(session, uri, (const char*)config.data);
		break;
	    }
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
	    else
	    {
		return ATOM_OK;
	    }
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_session_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_conn_handle* conn_handle;
    if (enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&conn_handle))
    {
        WT_CONNECTION* conn = conn_handle->conn;
        WT_SESSION* session;
        int rc = conn->open_session(conn, NULL, NULL, &session);
        if (rc == 0)
        {
            wterl_session_handle* session_handle =
		enif_alloc_resource(wterl_session_RESOURCE, sizeof(wterl_session_handle));
            session_handle->session = session;
            ERL_NIF_TERM result = enif_make_resource(env, session_handle);
            enif_keep_resource(conn_handle);
            enif_release_resource(session_handle);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
	    return wterl_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        int rc = session->close(session, NULL);
        if (rc != 0)
        {
	    return wterl_strerror(env, rc);
        }
	else
	{
	    return ATOM_OK;
	}
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_session_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_CREATE);
}

static ERL_NIF_TERM wterl_session_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_DROP);
}

static ERL_NIF_TERM wterl_session_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        ErlNifBinary config;
        Uri oldname, newname;
        if (enif_get_string(env, argv[1], oldname, sizeof oldname, ERL_NIF_LATIN1) &&
            enif_get_string(env, argv[2], newname, sizeof newname, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[3], &config))
        {
          int rc = session->rename(session, oldname, newname, (const char*)config.data);
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
	    else
	    {
		return ATOM_OK;
	    }
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_session_salvage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_SALVAGE);
}

static ERL_NIF_TERM wterl_session_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_SYNC);
}

static ERL_NIF_TERM wterl_session_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_TRUNCATE);
}

static ERL_NIF_TERM wterl_session_upgrade(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_UPGRADE);
}

static ERL_NIF_TERM wterl_session_verify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_session_worker(env, argc, argv, WTERL_OP_VERIFY);
}

static ERL_NIF_TERM wterl_session_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        Uri uri;
        ErlNifBinary key;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &key))
        {
            WT_CURSOR* cursor;
            int rc = session->open_cursor(session, uri, NULL, "raw", &cursor);
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
            WT_ITEM raw_key;
            raw_key.data = key.data;
            raw_key.size = key.size;
            cursor->set_key(cursor, &raw_key);
            rc = cursor->remove(cursor);
            cursor->close(cursor);
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
	    else
	    {
		return ATOM_OK;
	    }
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_session_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        Uri uri;
        ErlNifBinary key;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &key))
        {
            WT_CURSOR* cursor;
            int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", &cursor);
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
            WT_ITEM raw_key, raw_value;
            raw_key.data = key.data;
            raw_key.size = key.size;
            cursor->set_key(cursor, &raw_key);
            rc = cursor->search(cursor);
	    if (rc == 0)
                rc = cursor->get_value(cursor, &raw_value);
	    (void)cursor->close(cursor);
            if (rc == 0)
            {
                ErlNifBinary value;
                enif_alloc_binary(raw_value.size, &value);
                memcpy(value.data, raw_value.data, raw_value.size);
                return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &value));
            }
            else
            {
		return wterl_strerror(env, rc);
            }
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_session_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
        WT_SESSION* session = session_handle->session;
        Uri uri;
        ErlNifBinary key, value;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1) &&
            enif_inspect_binary(env, argv[2], &key) &&
	    enif_inspect_binary(env, argv[3], &value))
        {
            WT_CURSOR* cursor;
            int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", &cursor);
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
            WT_ITEM raw_key, raw_value;
            raw_key.data = key.data;
            raw_key.size = key.size;
            cursor->set_key(cursor, &raw_key);
            raw_value.data = value.data;
            raw_value.size = value.size;
            cursor->set_value(cursor, &raw_value);
            rc = cursor->insert(cursor);
            (void)cursor->close(cursor);
            if (rc != 0)
            {
		return wterl_strerror(env, rc);
            }
            return ATOM_OK;
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_session_handle* session_handle;
    if (enif_get_resource(env, argv[0], wterl_session_RESOURCE, (void**)&session_handle))
    {
	WT_SESSION* session = session_handle->session;
	WT_CURSOR* cursor;
	Uri uri;
        if (enif_get_string(env, argv[1], uri, sizeof uri, ERL_NIF_LATIN1))
	{
	    int rc = session->open_cursor(session, uri, NULL, "overwrite,raw", &cursor);
	    if (rc == 0)
	    {
		wterl_cursor_handle* cursor_handle =
			enif_alloc_resource(wterl_cursor_RESOURCE, sizeof(wterl_cursor_handle));
		cursor_handle->cursor = cursor;
		ERL_NIF_TERM result = enif_make_resource(env, cursor_handle);
		enif_keep_resource(session_handle);
		enif_release_resource(cursor_handle);
		return enif_make_tuple2(env, ATOM_OK, result);
	    }
	    else
	    {
		return wterl_strerror(env, rc);
	    }
	}
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_cursor_handle *cursor_handle;
    if (enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&cursor_handle))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
        int rc = cursor->close(cursor);
        if (rc == 0)
        {
            return ATOM_OK;
        }
        else
        {
	    return wterl_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_cursor_ret(ErlNifEnv* env, WT_CURSOR *cursor, int rc)
{
    if (rc == 0)
    {
	WT_ITEM raw_value;
	rc = cursor->get_value(cursor, &raw_value);
	if (rc == 0)
	{
	    ErlNifBinary value;
	    enif_alloc_binary(raw_value.size, &value);
	    memcpy(value.data, raw_value.data, raw_value.size);
	    return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &value));
	}
    }
    return wterl_strerror(env, rc);
}

static ERL_NIF_TERM wterl_cursor_np_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int prev)
{
    wterl_cursor_handle *cursor_handle;
    if (enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&cursor_handle))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
	return wterl_cursor_ret(env, cursor, prev == 0 ? cursor->next(cursor) : cursor->prev(cursor));
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_cursor_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_np_worker(env, argc, argv, 0);
}

static ERL_NIF_TERM wterl_cursor_prev(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_np_worker(env, argc, argv, 1);
}

static ERL_NIF_TERM wterl_cursor_search_worker(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int near)
{
    wterl_cursor_handle *cursor_handle;
    ErlNifBinary key;
    if (enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&cursor_handle) &&
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
	return wterl_cursor_ret(env, cursor,
				near == 1 ?
				cursor->search_near(cursor, &exact) : cursor->search(cursor));
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_cursor_search(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_search_worker(env, argc, argv, 0);
}

static ERL_NIF_TERM wterl_cursor_search_near(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_search_worker(env, argc, argv, 1);
}

static ERL_NIF_TERM wterl_cursor_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_cursor_handle *cursor_handle;
    if (enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&cursor_handle))
    {
	WT_CURSOR* cursor = cursor_handle->cursor;
        int rc = cursor->reset(cursor);
        if (rc == 0)
        {
            return ATOM_OK;
        }
        else
        {
	    return wterl_strerror(env, rc);
        }
    }
    return enif_make_badarg(env);
}

#define	WTERL_OP_CURSOR_INSERT		1
#define	WTERL_OP_CURSOR_UPDATE		2
#define	WTERL_OP_CURSOR_REMOVE		3

static inline ERL_NIF_TERM wterl_cursor_data_op(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], int op)
{
    wterl_cursor_handle *cursor_handle;
    if (enif_get_resource(env, argv[0], wterl_cursor_RESOURCE, (void**)&cursor_handle))
    {
        ErlNifBinary key, value;
	int rc;
        if (enif_inspect_binary(env, argv[1], &key) &&
	    enif_inspect_binary(env, argv[2], &value)) {
	    WT_CURSOR* cursor = cursor_handle->cursor;
	    WT_ITEM raw_key, raw_value;
	    raw_key.data = key.data;
	    raw_key.size = key.size;
	    cursor->set_key(cursor, &raw_key);
	    raw_value.data = value.data;
	    raw_value.size = value.size;
	    cursor->set_value(cursor, &raw_value);
	    switch (op) {
	    case WTERL_OP_CURSOR_INSERT:
		rc = cursor->insert(cursor);
		break;
	    case WTERL_OP_CURSOR_UPDATE:
		rc = cursor->update(cursor);
		break;
	    default:
	    case WTERL_OP_CURSOR_REMOVE:
		rc = cursor->remove(cursor);
		break;
	    }
	    if (rc == 0)
	    {
		return ATOM_OK;
	    }
	    else
	    {
		return wterl_strerror(env, rc);
	    }
	}
    }
    return enif_make_badarg(env);
}

static ERL_NIF_TERM wterl_cursor_insert(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_data_op(env, argc, argv, WTERL_OP_CURSOR_INSERT);
}

static ERL_NIF_TERM wterl_cursor_update(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_data_op(env, argc, argv, WTERL_OP_CURSOR_UPDATE);
}

static ERL_NIF_TERM wterl_cursor_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wterl_cursor_data_op(env, argc, argv, WTERL_OP_CURSOR_REMOVE);
}

static void wterl_conn_resource_cleanup(ErlNifEnv* env, void* arg)
{
    /* Delete any dynamically allocated memory stored in wterl_handle */
    /* wterl_handle* handle = (wterl_handle*)arg; */
}

static void wterl_session_resource_cleanup(ErlNifEnv* env, void* arg)
{
    /* Delete any dynamically allocated memory stored in wterl_handle */
    /* wterl_handle* handle = (wterl_handle*)arg; */
}

static void wterl_cursor_resource_cleanup(ErlNifEnv* env, void* arg)
{
    /* Delete any dynamically allocated memory stored in wterl_handle */
    /* wterl_handle* handle = (wterl_handle*)arg; */
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    wterl_conn_RESOURCE = enif_open_resource_type(env, NULL,
                                                  "wterl_conn_resource",
                                                  &wterl_conn_resource_cleanup,
                                                  flags, NULL);
    wterl_session_RESOURCE = enif_open_resource_type(env, NULL,
                                                     "wterl_session_resource",
                                                     &wterl_session_resource_cleanup,
                                                     flags, NULL);
    wterl_cursor_RESOURCE = enif_open_resource_type(env, NULL,
                                                     "wterl_cursor_resource",
                                                     &wterl_cursor_resource_cleanup,
                                                     flags, NULL);
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");
    return 0;
}

ERL_NIF_INIT(wterl, nif_funcs, &on_load, NULL, NULL, NULL);
