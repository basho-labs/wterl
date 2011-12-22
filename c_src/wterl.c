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

#include "wiredtiger.h"

static ErlNifResourceType* wterl_conn_RESOURCE;
static ErlNifResourceType* wterl_session_RESOURCE;

typedef struct
{
    WT_CONNECTION* conn;
} wterl_conn_handle;

typedef struct
{
    WT_SESSION* session;
} wterl_session_handle;

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ERROR;

// Prototypes
static ERL_NIF_TERM wterl_conn_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM wterl_session_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
{
    {"conn_open", 2, wterl_conn_open},
    {"session_new", 1, wterl_session_new},
    {"session_get", 2, wterl_session_get},
    {"session_put", 3, wterl_session_put},
    {"session_delete", 2, wterl_session_delete}
};


static ERL_NIF_TERM wterl_conn_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char homedir[4096];
    ErlNifBinary configBin;
    if (enif_get_string(env, argv[0], homedir, sizeof(homedir), ERL_NIF_LATIN1) &&
        enif_inspect_binary(env, argv[1], &configBin))
    {
        WT_CONNECTION* conn;
        int rc = wiredtiger_open(homedir, NULL, (const char*)configBin.data, &conn);
        if (rc == 0)
        {
            wterl_conn_handle* handle = enif_alloc_resource(wterl_conn_RESOURCE,
                                                       sizeof(wterl_conn_handle));
            handle->conn = conn;
            ERL_NIF_TERM result = enif_make_resource(env, handle);
            enif_release_resource(handle);
            return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
        }
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_string(env, wiredtiger_strerror(rc),
                                                     ERL_NIF_LATIN1));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }

}

static ERL_NIF_TERM wterl_session_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    wterl_conn_handle* conn_handle;
    if (enif_get_resource(env, argv[0], wterl_conn_RESOURCE, (void**)&conn_handle))
    {
        WT_CONNECTION* conn = conn_handle->conn;
        WT_SESSION* session;
        int rc = conn->open_session(conn, NULL, NULL, &session);
        if (rc == 0)
        {
            wterl_session_handle* shandle = enif_alloc_resource(wterl_session_RESOURCE,
                                                                sizeof(wterl_session_handle));
            shandle->session = session;
            ERL_NIF_TERM result = enif_make_resource(env, shandle);
            enif_keep_resource(conn_handle);
            enif_release_resource(shandle);
            return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
        }
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_string(env, wiredtiger_strerror(rc),
                                                     ERL_NIF_LATIN1));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM wterl_session_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM wterl_session_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM wterl_session_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_make_atom(env, "ok");
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
    ATOM_ERROR = enif_make_atom(env, "error");
    return 0;
}

ERL_NIF_INIT(wterl, nif_funcs, &on_load, NULL, NULL, NULL);
