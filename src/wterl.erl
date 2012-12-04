%% -------------------------------------------------------------------
%%
%% wterl: Erlang Wrapper for WiredTiger
%%
%% Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
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
         session_checkpoint/1,
         session_checkpoint/2,
         session_close/1,
         session_create/2,
         session_create/3,
         session_delete/3,
         session_drop/2,
         session_drop/3,
         session_get/3,
         session_open/1,
         session_open/2,
         session_put/4,
         session_rename/3,
         session_rename/4,
         session_salvage/2,
         session_salvage/3,
         session_truncate/2,
         session_truncate/3,
         session_upgrade/2,
         session_upgrade/3,
         session_verify/2,
         session_verify/3,
         config_value/3,
         config_to_bin/1,
         fold_keys/3,
         fold/3]).

-include("async_nif.hrl").

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

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
conn_open(HomeDir, Config) ->
    ?ASYNC_NIF_CALL(fun conn_open_nif/3, [HomeDir, Config]).

-spec conn_open_nif(reference(), string(), config()) -> {ok, connection()} | {error, term()}.
conn_open_nif(_AsyncRef, _HomeDir, _Config) ->
    ?nif_stub.

-spec conn_close(connection()) -> ok | {error, term()}.
conn_close(ConnRef) ->
    ?ASYNC_NIF_CALL(fun conn_close_nif/2, [ConnRef]).

-spec conn_close_nif(reference(), connection()) -> ok | {error, term()}.
conn_close_nif(_AsyncRef, _ConnRef) ->
    ?nif_stub.

-spec session_open(connection()) -> {ok, session()} | {error, term()}.
-spec session_open(connection(), config()) -> {ok, session()} | {error, term()}.
session_open(ConnRef) ->
    session_open(ConnRef, ?EMPTY_CONFIG).
session_open(ConnRef, Config) ->
    ?ASYNC_NIF_CALL(fun session_open_nif/3, [ConnRef, Config]).

-spec session_open_nif(reference(), connection(), config()) -> {ok, session()} | {error, term()}.
session_open_nif(_AsyncRef, _ConnRef, _Config) ->
    ?nif_stub.

-spec session_close(session()) -> ok | {error, term()}.
session_close(Ref) ->
    ?ASYNC_NIF_CALL(fun session_close_nif/2, [Ref]).

-spec session_close_nif(reference(), session()) -> ok | {error, term()}.
session_close_nif(_AsyncRef, _Ref) ->
    ?nif_stub.

-spec session_create(session(), string()) -> ok | {error, term()}.
-spec session_create(session(), string(), config()) -> ok | {error, term()}.
session_create(Ref, Name) ->
    session_create(Ref, Name, ?EMPTY_CONFIG).
session_create(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun session_create_nif/4, [Ref, Name, Config]).

-spec session_create_nif(reference(), session(), string(), config()) -> ok | {error, term()}.
session_create_nif(_AsyncNif, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_drop(session(), string()) -> ok | {error, term()}.
-spec session_drop(session(), string(), config()) -> ok | {error, term()}.
session_drop(Ref, Name) ->
    session_drop(Ref, Name, ?EMPTY_CONFIG).
session_drop(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun session_drop_nif/4, [Ref, Name, Config]).

-spec session_drop_nif(reference(), session(), string(), config()) -> ok | {error, term()}.
session_drop_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_delete(session(), string(), key()) -> ok | {error, term()}.
session_delete(Ref, Table, Key) ->
    ?ASYNC_NIF_CALL(fun session_delete_nif/4, [Ref, Table, Key]).

-spec session_delete_nif(reference(), session(), string(), key()) -> ok | {error, term()}.
session_delete_nif(_AsyncRef, _Ref, _Table, _Key) ->
    ?nif_stub.

-spec session_get(session(), string(), key()) -> {ok, value()} | not_found | {error, term()}.
session_get(Ref, Table, Key) ->
    ?ASYNC_NIF_CALL(fun session_get_nif/4, [Ref, Table, Key]).

-spec session_get_nif(reference(), session(), string(), key()) -> {ok, value()} | not_found | {error, term()}.
session_get_nif(_AsyncRef, _Ref, _Table, _Key) ->
    ?nif_stub.

-spec session_put(session(), string(), key(), value()) -> ok | {error, term()}.
session_put(Ref, Table, Key, Value) ->
    ?ASYNC_NIF_CALL(fun session_put_nif/5, [Ref, Table, Key, Value]).

-spec session_put_nif(reference(), session(), string(), key(), value()) -> ok | {error, term()}.
session_put_nif(_AsyncRef, _Ref, _Table, _Key, _Value) ->
    ?nif_stub.

-spec session_rename(session(), string(), string()) -> ok | {error, term()}.
-spec session_rename(session(), string(), string(), config()) -> ok | {error, term()}.
session_rename(Ref, OldName, NewName) ->
    session_rename(Ref, OldName, NewName, ?EMPTY_CONFIG).
session_rename(Ref, OldName, NewName, Config) ->
    ?ASYNC_NIF_CALL(fun session_rename_nif/5, [Ref, OldName, NewName, Config]).

-spec session_rename_nif(reference(), session(), string(), string(), config()) -> ok | {error, term()}.
session_rename_nif(_AsyncRef, _Ref, _OldName, _NewName, _Config) ->
    ?nif_stub.

-spec session_salvage(session(), string()) -> ok | {error, term()}.
-spec session_salvage(session(), string(), config()) -> ok | {error, term()}.
session_salvage(Ref, Name) ->
    session_salvage(Ref, Name, ?EMPTY_CONFIG).
session_salvage(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun session_salvage_nif/4, [Ref, Name, Config]).

-spec session_salvage_nif(reference(), session(), string(), config()) -> ok | {error, term()}.
session_salvage_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_checkpoint(session()) -> ok | {error, term()}.
-spec session_checkpoint(session(), config()) -> ok | {error, term()}.
session_checkpoint(_Ref) ->
    session_checkpoint(_Ref, ?EMPTY_CONFIG).
session_checkpoint(Ref, Config) ->
    ?ASYNC_NIF_CALL(fun session_checkpoint_nif/3, [Ref, Config]).

-spec session_checkpoint_nif(reference(), session(), config()) -> ok | {error, term()}.
session_checkpoint_nif(_AsyncRef, _Ref, _Config) ->
    ?nif_stub.

-spec session_truncate(session(), string()) -> ok | {error, term()}.
-spec session_truncate(session(), string(), config()) -> ok | {error, term()}.
session_truncate(Ref, Name) ->
    session_truncate(Ref, Name, ?EMPTY_CONFIG).
session_truncate(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun session_truncate_nif/4, [Ref, Name, Config]).

-spec session_truncate_nif(reference(), session(), string(), config()) -> ok | {error, term()}.
session_truncate_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_upgrade(session(), string()) -> ok | {error, term()}.
-spec session_upgrade(session(), string(), config()) -> ok | {error, term()}.
session_upgrade(Ref, Name) ->
    session_upgrade(Ref, Name, ?EMPTY_CONFIG).
session_upgrade(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun session_upgrade_nif/4, [Ref, Name, Config]).

-spec session_upgrade_nif(reference(), session(), string(), config()) -> ok | {error, term()}.
session_upgrade_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec session_verify(session(), string()) -> ok | {error, term()}.
-spec session_verify(session(), string(), config()) -> ok | {error, term()}.
session_verify(Ref, Name) ->
    session_verify(Ref, Name, ?EMPTY_CONFIG).
session_verify(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun session_verify_nif/4, [Ref, Name, Config]).

-spec session_verify_nif(reference(), session(), string(), config()) -> ok | {error, term()}.
session_verify_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec cursor_open(session(), string()) -> {ok, cursor()} | {error, term()}.
cursor_open(Ref, Table) ->
    ?ASYNC_NIF_CALL(fun cursor_open_nif/3, [Ref, Table]).

-spec cursor_open_nif(reference(), session(), string()) -> {ok, cursor()} | {error, term()}.
cursor_open_nif(_AsyncRef, _Ref, _Table) ->
    ?nif_stub.

-spec cursor_close(cursor()) -> ok | {error, term()}.
cursor_close(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_close_nif/2, [Cursor]).

-spec cursor_close_nif(reference(), cursor()) -> ok | {error, term()}.
cursor_close_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_next(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_next(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_next_nif/2, [Cursor]).

-spec cursor_next_nif(reference(), cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_next_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_next_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_next_key(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_next_key_nif/2, [Cursor]).

-spec cursor_next_key_nif(reference(), cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_next_key_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_next_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_next_value(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_next_value_nif/2, [Cursor]).

-spec cursor_next_value_nif(reference(), cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_next_value_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_prev(cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_prev(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_prev_nif/2, [Cursor]).

-spec cursor_prev_nif(reference(), cursor()) -> {ok, key(), value()} | not_found | {error, term()}.
cursor_prev_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_prev_key(cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_prev_key(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_prev_key_nif/2, [Cursor]).

-spec cursor_prev_key_nif(reference(), cursor()) -> {ok, key()} | not_found | {error, term()}.
cursor_prev_key_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_prev_value(cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_prev_value(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_prev_value_nif/2, [Cursor]).

-spec cursor_prev_value_nif(reference(), cursor()) -> {ok, value()} | not_found | {error, term()}.
cursor_prev_value_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_search(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search(Cursor, Key) ->
    ?ASYNC_NIF_CALL(fun cursor_search_nif/3, [Cursor, Key]).

-spec cursor_search_nif(reference(), cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search_nif(_AsyncRef, _Cursor, _Key) ->
    ?nif_stub.

-spec cursor_search_near(cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search_near(Cursor, Key) ->
    ?ASYNC_NIF_CALL(fun cursor_search_near_nif/3, [Cursor, Key]).

-spec cursor_search_near_nif(reference(), cursor(), key()) -> {ok, value()} | {error, term()}.
cursor_search_near_nif(_AsyncRef, _Cursor, _Key) ->
    ?nif_stub.

-spec cursor_reset(cursor()) -> ok | {error, term()}.
cursor_reset(Cursor) ->
    ?ASYNC_NIF_CALL(fun cursor_reset_nif/2, [Cursor]).

-spec cursor_reset_nif(reference(), cursor()) -> ok | {error, term()}.
cursor_reset_nif(_AsyncRef, _Cursor) ->
    ?nif_stub.

-spec cursor_insert(cursor(), key(), value()) -> ok | {error, term()}.
cursor_insert(Cursor, Key, Value) ->
    ?ASYNC_NIF_CALL(fun cursor_insert_nif/4, [Cursor, Key, Value]).

-spec cursor_insert_nif(reference(), cursor(), key(), value()) -> ok | {error, term()}.
cursor_insert_nif(_AsyncRef, _Cursor, _Key, _Value) ->
    ?nif_stub.

-spec cursor_update(cursor(), key(), value()) -> ok | {error, term()}.
cursor_update(Cursor, Key, Value) ->
    ?ASYNC_NIF_CALL(fun cursor_update_nif/4, [Cursor, Key, Value]).

-spec cursor_update_nif(reference(), cursor(), key(), value()) -> ok | {error, term()}.
cursor_update_nif(_AsyncRef, _Cursor, _Key, _Value) ->
    ?nif_stub.

-spec cursor_remove(cursor(), key(), value()) -> ok | {error, term()}.
cursor_remove(Cursor, Key, Value) ->
    ?ASYNC_NIF_CALL(fun cursor_remove_nif/4, [Cursor, Key, Value]).

-spec cursor_remove_nif(reference(), cursor(), key(), value()) -> ok | {error, term()}.
cursor_remove_nif(_AsyncRef, _Cursor, _Key, _Value) ->
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
    [{block_compressor, string},
     {cache_size, string},
     {create, bool},
     {direct_io, map},
     {drop, list},
     {error_prefix, string},
     {eviction_target, integer},
     {eviction_trigger, integer},
     {extensions, string},
     {force, bool},
     {hazard_max, integer},
     {home_environment, bool},
     {home_environment_priv, bool},
     {internal_page_max, string},
     {isolation, string},
     {key_type, string},
     {leaf_page_max, string},
     {logging, bool},
     {lsm_bloom_config, config},
     {lsm_chunk_size, string},
     {multiprocess, bool},
     {name, string},
     {session_max, integer},
     {target, list},
     {transactional, bool},
     {verbose, map}].

config_value(Key, Config, Default) ->
    {Key, app_helper:get_prop_or_env(Key, Config, wterl, Default)}.

config_encode(integer, Value) ->
    try
        list_to_binary(integer_to_list(Value))
    catch
        _:_ ->
            invalid
    end;
config_encode(config, Value) ->
    list_to_binary(["(", config_to_bin(Value, []), ")"]);
config_encode(list, Value) ->
    list_to_binary(["(", string:join(Value, ","), ")"]);
config_encode(map, Value) ->
    list_to_binary(["[", string:join(Value, ","), "]"]);
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
    iolist_to_binary([config_to_bin(Opts, []), <<"\0">>]).
config_to_bin([], Acc) ->
    iolist_to_binary(Acc);
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
               {"session checkpoint",
                fun() ->
                        Cfg = wterl:config_to_bin([{target, ["\"table:test\""]}]),
                        ?assertMatch(ok, session_checkpoint(SRef, Cfg)),
                        ?assertMatch({ok, <<"apple">>},
                                     session_get(SRef, "table:test", <<"a">>))
                end},
               {"session salvage",
                fun() ->
                        ok = session_salvage(SRef, "table:test"),
                        {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>)
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

-ifdef(EQC).

qc(P) ->
    ?assert(eqc:quickcheck(?QC_OUT(P))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _SRef, _Tbl, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], SRef, Tbl, Acc0) ->
    ok = wterl:session_put(SRef, Tbl, K, V),
    apply_kv_ops(Rest, SRef, Tbl, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], SRef, Tbl, Acc0) ->
    ok = case wterl:session_delete(SRef, Tbl, K) of
             ok ->
                 ok;
             not_found ->
                 ok;
             Else ->
                 Else
         end,
    apply_kv_ops(Rest, SRef, Tbl, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     DataDir = "/tmp/wterl.putdelete.qc",
                     Table = "table:eqc",
                     ?cmd("rm -rf "++DataDir),
                     ok = filelib:ensure_dir(filename:join(DataDir, "x")),
                     Cfg = wterl:config_to_bin([{create,true}]),
                     {ok, Conn} = wterl:conn_open(DataDir, Cfg),
                     {ok, SRef} = wterl:session_open(Conn),
                     try
                         wterl:session_create(SRef, Table),
                         Model = apply_kv_ops(Ops, SRef, Table, []),

                         %% Validate that all deleted values return not_found
                         F = fun({K, deleted}) ->
                                     ?assertEqual(not_found, wterl:session_get(SRef, Table, K));
                                ({K, V}) ->
                                     ?assertEqual({ok, V}, wterl:session_get(SRef, Table, K))
                             end,
                         lists:map(F, Model),
                         true
                     after
                         wterl:session_close(SRef),
                         wterl:conn_close(Conn)
                     end
                 end)).

prop_put_delete_test_() ->
    {timeout, 3*60, fun() -> qc(prop_put_delete()) end}.

-endif.

-endif.
