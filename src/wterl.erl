%% -------------------------------------------------------------------
%%
%% wterl: Erlang Wrapper for WiredTiger
%%
%% Copyright (c) 2011 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(wterl).
-export([conn_open/2,
         conn_close/1,
         cursor_close/1,
         cursor_insert/3,
         cursor_next/1,
         cursor_next_key/1,
         cursor_next_value/1,
         cursor_open/2,
         cursor_prev/1,
         cursor_prev_key/1,
         cursor_prev_value/1,
         cursor_remove/3,
         cursor_reset/1,
         cursor_search/2,
         cursor_search_near/2,
         cursor_update/3,
         session_close/1,
         session_create/2,
         session_create/3,
         session_delete/3,
         session_drop/2,
         session_drop/3,
         session_get/3,
         session_open/1,
         session_put/4,
         session_rename/3,
         session_rename/4,
         session_salvage/2,
         session_salvage/3,
         session_sync/2,
         session_sync/3,
         session_truncate/2,
         session_truncate/3,
         session_upgrade/2,
         session_upgrade/3,
         session_verify/2,
         session_verify/3,
         config_to_bin/1,
         fold_keys/3,
         fold/3]).

-type config() :: binary().
-type config_list() :: [{atom(), any()}].
-opaque connection() :: reference().
-opaque session() :: reference().
-opaque cursor() :: reference().
-type key() :: binary().
-type value() :: binary().

-export_type([connection/0, session/0]).

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

-define(EMPTY_CONFIG, <<"\0">>).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec init() -> ok | {error, any()}.
init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, bad_name} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    erlang:load_nif(filename:join(PrivDir, atom_to_list(?MODULE)), 0).

-spec conn_open(string(), config()) -> {ok, connection()} | {error, term()}.
conn_open(_HomeDir, _Config) ->
    ?nif_stub.

-spec conn_close(connection()) -> ok | {error, term()}.
conn_close(_ConnRef) ->
    ?nif_stub.

-spec session_open(connection()) -> {ok, session()} | {error, term()}.
session_open(_ConnRef) ->
    ?nif_stub.

-spec session_close(session()) -> ok | {error, term()}.
session_close(_Ref) ->
    ?nif_stub.

-spec session_create(session(), string()) -> ok | {error, term()}.
-spec session_create(session(), string(), config()) -> ok | {error, term()}.
session_create(Ref, Name) ->
    session_create(Ref, Name, ?EMPTY_CONFIG).
session_create(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_drop(session(), string()) -> ok | {error, term()}.
-spec session_drop(session(), string(), config()) -> ok | {error, term()}.
session_drop(Ref, Name) ->
    session_drop(Ref, Name, ?EMPTY_CONFIG).
session_drop(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_delete(session(), string(), key()) -> ok | {error, term()}.
session_delete(_Ref, _Table, _Key) ->
    ?nif_stub.

-spec session_get(session(), string(), key()) -> {ok, value()} | not_found | {error, term()}.
session_get(_Ref, _Table, _Key) ->
    ?nif_stub.

-spec session_put(session(), string(), key(), value()) -> ok | {error, term()}.
session_put(_Ref, _Table, _Key, _Value) ->
    ?nif_stub.

-spec session_rename(session(), string(), string()) -> ok | {error, term()}.
-spec session_rename(session(), string(), string(), config()) -> ok | {error, term()}.
session_rename(Ref, OldName, NewName) ->
    session_rename(Ref, OldName, NewName, ?EMPTY_CONFIG).
session_rename(_Ref, _OldName, _NewName, _Config) ->
    ?nif_stub.

-spec session_salvage(session(), string()) -> ok | {error, term()}.
-spec session_salvage(session(), string(), config()) -> ok | {error, term()}.
session_salvage(Ref, Name) ->
    session_salvage(Ref, Name, ?EMPTY_CONFIG).
session_salvage(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_sync(session(), string()) -> ok | {error, term()}.
-spec session_sync(session(), string(), config()) -> ok | {error, term()}.
session_sync(Ref, Name) ->
    session_sync(Ref, Name, ?EMPTY_CONFIG).
session_sync(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_truncate(session(), string()) -> ok | {error, term()}.
-spec session_truncate(session(), string(), config()) -> ok | {error, term()}.
session_truncate(Ref, Name) ->
    session_truncate(Ref, Name, ?EMPTY_CONFIG).
session_truncate(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_upgrade(session(), string()) -> ok | {error, term()}.
-spec session_upgrade(session(), string(), config()) -> ok | {error, term()}.
session_upgrade(Ref, Name) ->
    session_upgrade(Ref, Name, ?EMPTY_CONFIG).
session_upgrade(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_verify(session(), string()) -> ok | {error, term()}.
-spec session_verify(session(), string(), config()) -> ok | {error, term()}.
session_verify(Ref, Name) ->
    session_verify(Ref, Name, ?EMPTY_CONFIG).
session_verify(_Ref, _Name, _Config) ->
    ?nif_stub.

-spec cursor_open(session(), string()) -> {ok, cursor()} | {error, term()}.
cursor_open(_Ref, _Table) ->
    ?nif_stub.

-spec cursor_close(cursor()) -> ok | {error, term()}.
cursor_close(_Cursor) ->
    ?nif_stub.

-spec cursor_next(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_next(_Cursor) ->
    ?nif_stub.

-spec cursor_next_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_next_key(_Cursor) ->
    ?nif_stub.

-spec cursor_next_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_next_value(_Cursor) ->
    ?nif_stub.

-spec cursor_prev(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_prev(_Cursor) ->
    ?nif_stub.

-spec cursor_prev_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_prev_key(_Cursor) ->
    ?nif_stub.

-spec cursor_prev_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_prev_value(_Cursor) ->
    ?nif_stub.

-spec cursor_search(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search(_Cursor, _Key) ->
    ?nif_stub.

-spec cursor_search_near(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search_near(_Cursor, _Key) ->
    ?nif_stub.

-spec cursor_reset(cursor()) -> ok | {error, term()}.
cursor_reset(_Cursor) ->
    ?nif_stub.

-spec cursor_insert(cursor(), key(), value()) -> ok | {error, term()}.
cursor_insert(_Cursor, _Key, _Value) ->
    ?nif_stub.

-spec cursor_update(cursor(), key(), value()) -> ok | {error, term()}.
cursor_update(_Cursor, _Key, _Value) ->
    ?nif_stub.

-spec cursor_remove(cursor(), key(), value()) -> ok | {error, term()}.
cursor_remove(_Cursor, _Key, _Value) ->
    ?nif_stub.

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

-spec fold_keys(cursor(), fold_keys_fun(), any()) -> any().
fold_keys(Cursor, Fun, Acc0) ->
    fold_keys(Cursor, Fun, Acc0, cursor_next_key(Cursor)).
fold_keys(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold_keys(Cursor, Fun, Acc, {ok, Key}) ->
    fold_keys(Cursor, Fun, Fun(Key, Acc), cursor_next_key(Cursor)).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

-spec fold(cursor(), fold_fun(), any()) -> any().
fold(Cursor, Fun, Acc0) ->
    fold(Cursor, Fun, Acc0, cursor_next(Cursor)).
fold(_Cursor, _Fun, Acc, not_found) ->
    Acc;
fold(Cursor, Fun, Acc, {ok, Key, Value}) ->
    fold(Cursor, Fun, Fun({Key, Value}, Acc), cursor_next(Cursor)).

%%
%% Configuration type information.
%%
config_types() ->
    [{cache_size, string},
     {create, bool},
     {error_prefix, string},
     {eviction_target, integer},
     {eviction_trigger, integer},
     {extensions, string},
     {force, bool},
     {hazard_max, integer},
     {home_environment, bool},
     {home_environment_priv, bool},
     {logging, bool},
     {multiprocess, bool},
     {session_max, integer},
     {transactional, bool},
     {verbose, string}].

config_encode(integer, Value) ->
    try
        list_to_binary(integer_to_list(Value))
    catch
        _:_ ->
            invalid
    end;
config_encode(string, Value) ->
    list_to_binary(Value);
config_encode(bool, true) ->
    <<"true">>;
config_encode(bool, false) ->
    <<"false">>;
config_encode(_Type, _Value) ->
    invalid.

-spec config_to_bin(config_list()) -> config().
config_to_bin(Opts) ->
    config_to_bin(Opts, []).
config_to_bin([], Acc) ->
    iolist_to_binary([Acc, ?EMPTY_CONFIG]);
config_to_bin([{Key, Value} | Rest], Acc) ->
    case lists:keysearch(Key, 1, config_types()) of
        {value, {Key, Type}} ->
            Acc2 = case config_encode(Type, Value) of
                       invalid ->
                           error_logger:error_msg("Skipping invalid option ~p = ~p\n",
                                                  [Key, Value]),
                           Acc;
                       EncodedValue ->
                           EncodedKey = atom_to_binary(Key, utf8),
                           [EncodedKey, <<"=">>, EncodedValue, <<",">> | Acc]
                   end,
            config_to_bin(Rest, Acc2);
        false ->
            error_logger:error_msg("Skipping unknown option ~p = ~p\n", [Key, Value]),
            config_to_bin(Rest, Acc)
    end.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(TEST_DATA_DIR, "test/wterl.basic").

open_test_conn(DataDir) ->
    ?assertCmd("rm -rf "++DataDir),
    ?assertMatch(ok, filelib:ensure_dir(filename:join(DataDir, "x"))),
    OpenConfig = config_to_bin([{create,true},{cache_size,"100MB"}]),
    {ok, ConnRef} = conn_open(DataDir, OpenConfig),
    ConnRef.

open_test_session(ConnRef) ->
    {ok, SRef} = session_open(ConnRef),
    ?assertMatch(ok, session_drop(SRef, "table:test", config_to_bin([{force,true}]))),
    ?assertMatch(ok, session_create(SRef, "table:test")),
    SRef.

conn_test() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    ?assertMatch(ok, conn_close(ConnRef)).

session_test_() ->
    {setup,
     fun() ->
             open_test_conn(?TEST_DATA_DIR)
     end,
     fun(ConnRef) ->
             ok = conn_close(ConnRef)
     end,
     fun(ConnRef) ->
             {inorder,
              [{"open/close a session",
                fun() ->
                        {ok, SRef} = session_open(ConnRef),
                        ?assertMatch(ok, session_close(SRef))
                end},
               {"create and drop a table",
                fun() ->
                        SRef = open_test_session(ConnRef),
                        ?assertMatch(ok, session_drop(SRef, "table:test")),
                        ?assertMatch(ok, session_close(SRef))
                end}]}
     end}.

insert_delete_test() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    SRef = open_test_session(ConnRef),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"a">>, <<"apple">>)),
    ?assertMatch({ok, <<"apple">>}, session_get(SRef, "table:test", <<"a">>)),
    ?assertMatch(ok,  session_delete(SRef, "table:test", <<"a">>)),
    ?assertMatch(not_found,  session_get(SRef, "table:test", <<"a">>)),
    ok = session_close(SRef),
    ok = conn_close(ConnRef).

init_test_table() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    SRef = open_test_session(ConnRef),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"a">>, <<"apple">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"b">>, <<"banana">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"c">>, <<"cherry">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"d">>, <<"date">>)),
    ?assertMatch(ok, session_put(SRef, "table:test", <<"g">>, <<"gooseberry">>)),
    {ConnRef, SRef}.

stop_test_table({ConnRef, SRef}) ->
    ?assertMatch(ok, session_close(SRef)),
    ?assertMatch(ok, conn_close(ConnRef)).

various_session_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, SRef}) ->
             {inorder,
              [{"session verify",
                fun() ->
                        ?assertMatch(ok, session_verify(SRef, "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session sync",
                fun() ->
                        ?assertMatch(ok, session_sync(SRef, "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session salvage",
                fun() ->
                        %% ===============================================================
                        %% KEITH: SKIP SALVAGE FOR NOW, THERE IS SOMETHING WRONG.
                        %% ===============================================================
                        %% ok = session_salvage(SRef, "table:test"),
                        %% {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),
                        ok
                end},
               {"session upgrade",
                fun() ->
                        ?assertMatch(ok, session_upgrade(SRef, "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session rename",
                fun() ->
                        ?assertMatch(ok,
                                     session_rename(SRef, "table:test", "table:new")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:new", <<"a">>)),
                        ?assertMatch(ok,
                                     session_rename(SRef, "table:new", "table:test")),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session truncate",
                fun() ->
                        ?assertMatch(ok, session_truncate(SRef, "table:test")),
                        ?assertMatch(not_found, session_get(SRef, "table:test", <<"a">>))
                end}]}
     end}.

cursor_open_close_test() ->
    {ConnRef, SRef} = init_test_table(),
    {ok, Cursor1} = cursor_open(SRef, "table:test"),
    ?assertMatch({ok, <<"a">>, <<"apple">>}, cursor_next(Cursor1)),
    ?assertMatch(ok, cursor_close(Cursor1)),
    {ok, Cursor2} = cursor_open(SRef, "table:test"),
    ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, cursor_prev(Cursor2)),
    ?assertMatch(ok, cursor_close(Cursor2)),
    stop_test_table({ConnRef, SRef}).

various_cursor_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun({_, SRef}) ->
             {inorder,
              [{"move a cursor back and forth, getting key",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"a">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"b">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, cursor_prev_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"g">>}, cursor_next_key(Cursor)),
                        ?assertMatch(not_found, cursor_next_key(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting value",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"banana">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, cursor_prev_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"gooseberry">>}, cursor_next_value(Cursor)),
                        ?assertMatch(not_found, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting key and value",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"a">>, <<"apple">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"b">>, <<"banana">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, cursor_prev(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, cursor_next(Cursor)),
                        ?assertMatch(not_found, cursor_next(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"fold keys",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch([<<"g">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
                                     fold_keys(Cursor, fun(Key, Acc) -> [Key | Acc] end, [])),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"search for an item",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"banana">>}, cursor_search(Cursor, <<"b">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"range search for an item",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"gooseberry">>},
                                     cursor_search_near(Cursor, <<"z">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"check cursor reset",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_reset(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"insert/overwrite an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch(ok,
                                     cursor_insert(Cursor, <<"h">>, <<"huckleberry">>)),
                        ?assertMatch({ok, <<"huckleberry">>},
                                     cursor_search(Cursor, <<"h">>)),
                        ?assertMatch(ok,
                                     cursor_insert(Cursor, <<"g">>, <<"grapefruit">>)),
                        ?assertMatch({ok, <<"grapefruit">>},
                                     cursor_search(Cursor, <<"g">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch({ok, <<"grapefruit">>},
                                     session_get(SRef, "table:test", <<"g">>)),
                        ?assertMatch({ok, <<"huckleberry">>},
                                     session_get(SRef, "table:test", <<"h">>))
                end},
               {"update an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch(ok,
                                     cursor_update(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found,
                                     cursor_update(Cursor, <<"k">>, <<"kumquat">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch({ok, <<"goji berries">>},
                                     session_get(SRef, "table:test", <<"g">>))
                end},
               {"remove an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(SRef, "table:test"),
                        ?assertMatch(ok,
                                     cursor_remove(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found,
                                     cursor_remove(Cursor, <<"l">>, <<"lemon">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch(not_found,
                                     session_get(SRef, "table:test", <<"g">>))
                end}]}
     end}.

-endif.
