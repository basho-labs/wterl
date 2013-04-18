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

-export([connection_open/2,
         connection_open/3,
         connection_close/1,
         cursor_close/1,
         cursor_insert/3,
         cursor_next/1,
         cursor_next_key/1,
         cursor_next_value/1,
         cursor_open/2,
         cursor_open/3,
         cursor_prev/1,
         cursor_prev_key/1,
         cursor_prev_value/1,
         cursor_remove/2,
         cursor_reset/1,
         cursor_search/2,
         cursor_search/3,
         cursor_search_near/2,
         cursor_search_near/3,
         cursor_update/3,
         checkpoint/1,
         checkpoint/2,
         create/2,
         create/3,
         delete/3,
         drop/2,
         drop/3,
         get/3,
         put/4,
         rename/3,
         rename/4,
         salvage/2,
         salvage/3,
         truncate/2,
         truncate/3,
         truncate/4,
         truncate/5,
         upgrade/2,
         upgrade/3,
         verify/2,
         verify/3,
         config_value/3,
         priv_dir/0,
         fold_keys/3,
         fold/3]).

-include("async_nif.hrl").

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P), eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-type config() :: binary().
-type config_list() :: [{atom(), any()}].
-opaque connection() :: reference().
-opaque cursor() :: reference().
-type key() :: binary().
-type value() :: binary().

-export_type([connection/0, cursor/0]).

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

-spec init() -> ok | {error, any()}.
init() ->
    erlang:load_nif(filename:join([priv_dir(), atom_to_list(?MODULE)]),
                    [{wterl, "07061ed6e8252543c2f06b81a646eca6945cc558"},
                     {wiredtiger, "6f7a4b961c744bfb21f0c21d4c28c2d162400f1b"}]).

-spec connection_open(string(), config_list()) -> {ok, connection()} | {error, term()}.
-spec connection_open(string(), config_list(), config_list()) -> {ok, connection()} | {error, term()}.
connection_open(HomeDir, ConnectionConfig) ->
    connection_open(HomeDir, ConnectionConfig, []).
connection_open(HomeDir, ConnectionConfig, SessionConfig) ->
    PrivDir = wterl:priv_dir(),
    {ok, PrivFiles} = file:list_dir(PrivDir),
    SoFiles =
        lists:filter(fun(Elem) ->
                             case re:run(Elem, "^libwiredtiger_.*\.so$") of
                                 {match, _} -> true;
                                 nomatch -> false
                             end
                     end, PrivFiles),
    SoPaths = lists:map(fun(Elem) -> filename:join([PrivDir, Elem]) end, SoFiles),
    conn_open(HomeDir, [{extensions, SoPaths}] ++ ConnectionConfig, SessionConfig).

-spec conn_open(string(), config_list(), config_list()) -> {ok, connection()} | {error, term()}.
conn_open(HomeDir, ConnectionConfig, SessionConfig) ->
    ?ASYNC_NIF_CALL(fun conn_open_nif/4, [HomeDir,
                                          config_to_bin(ConnectionConfig),
                                          config_to_bin(SessionConfig)]).

-spec conn_open_nif(reference(), string(), config(), config()) -> {ok, connection()} | {error, term()}.
conn_open_nif(_AsyncRef, _HomeDir, _ConnectionConfig, _SessionConfig) ->
    ?nif_stub.

-spec connection_close(connection()) -> ok | {error, term()}.
connection_close(ConnRef) ->
    ?ASYNC_NIF_CALL(fun conn_close_nif/2, [ConnRef]).

-spec conn_close_nif(reference(), connection()) -> ok | {error, term()}.
conn_close_nif(_AsyncRef, _ConnRef) ->
    ?nif_stub.

-spec create(connection(), string()) -> ok | {error, term()}.
-spec create(connection(), string(), config_list()) -> ok | {error, term()}.
create(Ref, Name) ->
    create(Ref, Name, []).
create(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun create_nif/4, [Ref, Name, config_to_bin(Config)]).

-spec create_nif(reference(), connection(), string(), config()) -> ok | {error, term()}.
create_nif(_AsyncNif, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec drop(connection(), string()) -> ok | {error, term()}.
-spec drop(connection(), string(), config_list()) -> ok | {error, term()}.
drop(Ref, Name) ->
    drop(Ref, Name, []).
drop(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun drop_nif/4, [Ref, Name, config_to_bin(Config)]).

-spec drop_nif(reference(), connection(), string(), config()) -> ok | {error, term()}.
drop_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec delete(connection(), string(), key()) -> ok | {error, term()}.
delete(Ref, Table, Key) ->
    ?ASYNC_NIF_CALL(fun delete_nif/4, [Ref, Table, Key]).

-spec delete_nif(reference(), connection(), string(), key()) -> ok | {error, term()}.
delete_nif(_AsyncRef, _Ref, _Table, _Key) ->
    ?nif_stub.

-spec get(connection(), string(), key()) -> {ok, value()} | not_found | {error, term()}.
get(Ref, Table, Key) ->
    ?ASYNC_NIF_CALL(fun get_nif/4, [Ref, Table, Key]).

-spec get_nif(reference(), connection(), string(), key()) -> {ok, value()} | not_found | {error, term()}.
get_nif(_AsyncRef, _Ref, _Table, _Key) ->
    ?nif_stub.

-spec put(connection(), string(), key(), value()) -> ok | {error, term()}.
put(Ref, Table, Key, Value) ->
    ?ASYNC_NIF_CALL(fun put_nif/5, [Ref, Table, Key, Value]).

-spec put_nif(reference(), connection(), string(), key(), value()) -> ok | {error, term()}.
put_nif(_AsyncRef, _Ref, _Table, _Key, _Value) ->
    ?nif_stub.

-spec rename(connection(), string(), string()) -> ok | {error, term()}.
-spec rename(connection(), string(), string(), config_list()) -> ok | {error, term()}.
rename(Ref, OldName, NewName) ->
    rename(Ref, OldName, NewName, []).
rename(Ref, OldName, NewName, Config) ->
    ?ASYNC_NIF_CALL(fun rename_nif/5, [Ref, OldName, NewName, config_to_bin(Config)]).

-spec rename_nif(reference(), connection(), string(), string(), config()) -> ok | {error, term()}.
rename_nif(_AsyncRef, _Ref, _OldName, _NewName, _Config) ->
    ?nif_stub.

-spec salvage(connection(), string()) -> ok | {error, term()}.
-spec salvage(connection(), string(), config_list()) -> ok | {error, term()}.
salvage(Ref, Name) ->
    salvage(Ref, Name, []).
salvage(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun salvage_nif/4, [Ref, Name, config_to_bin(Config)]).

-spec salvage_nif(reference(), connection(), string(), config()) -> ok | {error, term()}.
salvage_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec checkpoint(connection()) -> ok | {error, term()}.
-spec checkpoint(connection(), config_list()) -> ok | {error, term()}.
checkpoint(_Ref) ->
    checkpoint(_Ref, []).
checkpoint(Ref, Config) ->
    ?ASYNC_NIF_CALL(fun checkpoint_nif/3, [Ref, config_to_bin(Config)]).

-spec checkpoint_nif(reference(), connection(), config()) -> ok | {error, term()}.
checkpoint_nif(_AsyncRef, _Ref, _Config) ->
    ?nif_stub.

-spec truncate(connection(), string()) -> ok | {error, term()}.
-spec truncate(connection(), string(), config_list()) -> ok | {error, term()}.
-spec truncate(connection(), string(), binary() | first, binary() | last) -> ok | {error, term()}.
-spec truncate(connection(), string(), binary() | first, binary() | last, config()) -> ok | {error, term()}.
truncate(Ref, Name) ->
    truncate(Ref, Name, first, last, []).
truncate(Ref, Name, Config) ->
    truncate(Ref, Name, first, last, Config).
truncate(Ref, Name, Start, Stop) ->
    truncate(Ref, Name, Start, Stop, []).
truncate(Ref, Name, Start, Stop, Config) ->
    ?ASYNC_NIF_CALL(fun truncate_nif/6, [Ref, Name, Start, Stop, config_to_bin(Config)]).

-spec truncate_nif(reference(), connection(), string(), cursor() | first, cursor() | last, config()) -> ok | {error, term()}.
truncate_nif(_AsyncRef, _Ref, _Name, _Start, _Stop, _Config) ->
    ?nif_stub.

-spec upgrade(connection(), string()) -> ok | {error, term()}.
-spec upgrade(connection(), string(), config_list()) -> ok | {error, term()}.
upgrade(Ref, Name) ->
    upgrade(Ref, Name, []).
upgrade(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun upgrade_nif/4, [Ref, Name, config_to_bin(Config)]).

-spec upgrade_nif(reference(), connection(), string(), config()) -> ok | {error, term()}.
upgrade_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec verify(connection(), string()) -> ok | {error, term()}.
-spec verify(connection(), string(), config_list()) -> ok | {error, term()}.
verify(Ref, Name) ->
    verify(Ref, Name, []).
verify(Ref, Name, Config) ->
    ?ASYNC_NIF_CALL(fun verify_nif/4, [Ref, Name, config_to_bin(Config)]).

-spec verify_nif(reference(), connection(), string(), config()) -> ok | {error, term()}.
verify_nif(_AsyncRef, _Ref, _Name, _Config) ->
    ?nif_stub.

-spec cursor_open(connection(), string()) -> {ok, cursor()} | {error, term()}.
-spec cursor_open(connection(), string(), config_list()) -> {ok, cursor()} | {error, term()}.
cursor_open(Ref, Table) ->
    cursor_open(Ref, Table, []).
cursor_open(Ref, Table, Config) ->
    ?ASYNC_NIF_CALL(fun cursor_open_nif/4, [Ref, Table, config_to_bin(Config)]).

-spec cursor_open_nif(reference(), connection(), string(), config()) -> {ok, cursor()} | {error, term()}.
cursor_open_nif(_AsyncRef, _Ref, _Table, _Config) ->
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
-spec cursor_search(cursor(), key(), boolean()) -> {ok, value()} | {error, term()}.
cursor_search(Cursor, Key) ->
    ?ASYNC_NIF_CALL(fun cursor_search_nif/4, [Cursor, Key, false]).
cursor_search(Cursor, Key, Scanning) when is_boolean(Scanning) ->
    ?ASYNC_NIF_CALL(fun cursor_search_nif/4, [Cursor, Key, Scanning]).

-spec cursor_search_nif(reference(), cursor(), key(), boolean()) -> {ok, value()} | {error, term()}.
cursor_search_nif(_AsyncRef, _Cursor, _Key, _Scanning) ->
    ?nif_stub.

-spec cursor_search_near(cursor(), key()) -> {ok, value()} | {error, term()}.
-spec cursor_search_near(cursor(), key(), boolean()) -> {ok, value()} | {error, term()}.
cursor_search_near(Cursor, Key) ->
    ?ASYNC_NIF_CALL(fun cursor_search_near_nif/4, [Cursor, Key, false]).
cursor_search_near(Cursor, Key, Scanning) when is_boolean(Scanning) ->
    ?ASYNC_NIF_CALL(fun cursor_search_near_nif/4, [Cursor, Key, Scanning]).

-spec cursor_search_near_nif(reference(), cursor(), key(), boolean()) -> {ok, value()} | {error, term()}.
cursor_search_near_nif(_AsyncRef, _Cursor, _Key, _Scanning) ->
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

-spec cursor_remove(cursor(), key()) -> ok | {error, term()}.
cursor_remove(Cursor, Key) ->
    ?ASYNC_NIF_CALL(fun cursor_remove_nif/3, [Cursor, Key]).

-spec cursor_remove_nif(reference(), cursor(), key()) -> ok | {error, term()}.
cursor_remove_nif(_AsyncRef, _Cursor, _Key) ->
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

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join([AppPath, "priv"]);
        Path ->
            Path
    end.

%%
%% Configuration information.
%%

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
config_encode({list, quoted}, Value) ->
    Values = lists:map(fun(S) -> "\"" ++ S ++ "\"" end, Value),
    list_to_binary(["(", string:join(Values, ","), ")"]);
config_encode(string, Value) when is_list(Value) ->
    list_to_binary(Value);
config_encode({string, quoted}, Value) when is_list(Value) ->
    list_to_binary("\"" ++ Value ++ "\"");
config_encode(string, Value) when is_number(Value) ->
    list_to_binary(integer_to_list(Value));
config_encode(bool, true) ->
    <<"true">>;
config_encode(bool, Value) when is_number(Value) andalso Value =/= 0 ->
    <<"true">>;
config_encode(bool, "true") ->
    <<"true">>;
config_encode(bool, false) ->
    <<"false">>;
config_encode(bool, 0) ->
    <<"false">>;
config_encode(bool, "false") ->
    <<"false">>;
config_encode(_Type, _Value) ->
    invalid.

-spec config_to_bin(config_list()) -> config().
config_to_bin(Opts) ->
    iolist_to_binary([config_to_bin(Opts, []), <<"\0">>]).
config_to_bin([], Acc) ->
    iolist_to_binary(Acc);
config_to_bin([{Key, Value} | Rest], Acc) ->
    ConfigTypes =
    [{block_compressor, {string, quoted}},
     {cache_size, string},
     {checkpoint, config},
     {create, bool},
     {direct_io, list},
     {drop, list},
     {error_prefix, string},
     {eviction_target, integer},
     {eviction_trigger, integer},
     {extensions, {list, quoted}},
     {force, bool},
     {from, string},
     {hazard_max, integer},
     {home_environment, bool},
     {home_environment_priv, bool},
     {internal_page_max, string},
     {isolation, string},
     {key_type, string},
     {leaf_page_max, string},
     {logging, bool},
     {lsm_bloom_bit_count, integer},
     {lsm_bloom_config, config},
     {lsm_bloom_hash_count, integer},
     {lsm_bloom_newest, bool},
     {lsm_bloom_oldest, bool},
     {lsm_chunk_size, string},
     {prefix_compression, bool},
     {lsm_merge_threads, integer},
     {multiprocess, bool},
     {name, string},
     {session_max, integer},
     {statistics_log, config},
     {sync, bool},
     {target, {list, quoted}},
     {to, string},
     {transactional, bool},
     {verbose, list},
     {wait, integer}],

    case lists:keysearch(Key, 1, ConfigTypes) of
        {value, {Key, Type}} ->
            Acc2 = case config_encode(Type, Value) of
                       invalid ->
                           error_logger:error_msg("Skipping invalid option ~p = ~p\n", [Key, Value]),
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
    open_test_conn(DataDir, [{create,true},{cache_size,"100MB"},{session_max, 8192}]).
open_test_conn(DataDir, OpenConfig) ->
    {ok, CWD} = file:get_cwd(),
    rmdir:path(filename:join([CWD, DataDir])), %?cmd("rm -rf " ++ filename:join([CWD, DataDir])),
    ?assertMatch(ok, filelib:ensure_dir(filename:join([DataDir, "x"]))),
    {ok, ConnRef} = connection_open(filename:join([CWD, DataDir]), OpenConfig),
    ConnRef.

open_test_table(ConnRef) ->
    open_test_table(ConnRef, "table", []).

open_test_table(ConnRef, Type) ->
    open_test_table(ConnRef, Type, []).

open_test_table(ConnRef, Type, Opts) ->
    ?assertMatch(ok, create(ConnRef, Type ++ ":test", Opts)),
    ConnRef.

conn_test_() ->
    {setup,
     fun() ->
             open_test_conn(?TEST_DATA_DIR)
     end,
     fun(ConnRef) ->
             ok = connection_close(ConnRef)
     end,
     fun(ConnRef) ->
             {inorder,
              [{"open and close a connection",
                fun() ->
                        ConnRef = open_test_table(ConnRef)
                end},
               {"create, verify, drop a table(btree)",
                fun() ->
                        wterl:create(ConnRef, "table:test", []),
                        ?assertMatch(ok, verify(ConnRef, "table:test")),
                        ?assertMatch(ok, drop(ConnRef, "table:test"))
                end},
               {"create, test verify, drop a table(lsm)",
                fun() ->
                        ConnRef = open_test_table(ConnRef, "lsm"),
                        ?assertMatch(ok, verify(ConnRef, "lsm:test")),
                        ?assertMatch(ok, drop(ConnRef, "lsm:test"))
                end},
               {"create, verify, drop a table(btree, snappy)",
                fun() ->
                        ConnRef = open_test_table(ConnRef, "table", [{block_compressor, "snappy"}]),
                        ?assertMatch(ok, verify(ConnRef, "table:test")),
                        ?assertMatch(ok, drop(ConnRef, "table:test"))
                end},
               {"create, verify, drop a table(btree, bzip2)",
                fun() ->
                        ConnRef = open_test_table(ConnRef, "table", [{block_compressor, "bzip2"}]),
                        ?assertMatch(ok, verify(ConnRef, "table:test")),
                        ?assertMatch(ok, drop(ConnRef, "table:test"))
                end}
              ]}
     end}.

insert_delete_test() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    ConnRef = open_test_table(ConnRef),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"a">>, <<"apple">>)),
    ?assertMatch({ok, <<"apple">>}, get(ConnRef, "table:test", <<"a">>)),
    ?assertMatch(ok, delete(ConnRef, "table:test", <<"a">>)),
    ?assertMatch(not_found,  get(ConnRef, "table:test", <<"a">>)),
    ok = connection_close(ConnRef).

init_test_table() ->
    ConnRef = open_test_conn(?TEST_DATA_DIR),
    ConnRef = open_test_table(ConnRef),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"a">>, <<"apple">>)),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"b">>, <<"banana">>)),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"c">>, <<"cherry">>)),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"d">>, <<"date">>)),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"e">>, <<"elephant">>)),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"f">>, <<"forest">>)),
    ?assertMatch(ok, put(ConnRef, "table:test", <<"g">>, <<"gooseberry">>)),
    ConnRef.

stop_test_table(ConnRef) ->
    ?assertMatch(ok, connection_close(ConnRef)).

various_online_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun(ConnRef) ->
             {inorder,
              [
               {"checkpoint",
                fun() ->
                        ?assertMatch(ok, checkpoint(ConnRef, [{target, ["table:test"]}])),
                        ?assertMatch({ok, <<"apple">>}, get(ConnRef, "table:test", <<"a">>))
                end},
               {"truncate entire table",
                fun() ->
                        ?assertMatch(ok, truncate(ConnRef, "table:test")),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"a">>))
                end},
               {"truncate range [<<b>>..last], ensure value outside range is found after",
                fun() ->
                        ?assertMatch(ok, truncate(ConnRef, "table:test", <<"b">>, last)),
                        ?assertMatch({ok, <<"apple">>}, get(ConnRef, "table:test", <<"a">>))
                end},
               {"truncate range [first..<<b>>], ensure value inside range is not_found after",
                fun() ->
                        ?assertMatch(ok, truncate(ConnRef, "table:test", first, <<"b">>)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"a">>))
                end},
               {"truncate range [first..not_found] with a key that doesn't exist",
                fun() ->
                        ?assertMatch(not_found, truncate(ConnRef, "table:test", first, <<"z">>))
                end},
               {"truncate range [not_found..last] with a key that doesn't exist",
                fun() ->
                        ?assertMatch(not_found, truncate(ConnRef, "table:test", <<"0">>, last))
                end},
               {"truncate range [not_found..not_found] with keys that don't exist",
                fun() ->
                        ?assertMatch(not_found, truncate(ConnRef, "table:test", <<"0">>, <<"0">>))
                end},
               {"truncate range [<<b>...<<f>>], ensure value before & after range still exist",
                fun() ->
                        ?assertMatch(ok, truncate(ConnRef, "table:test", <<"b">>, <<"f">>)),
                        ?assertMatch({ok, <<"apple">>}, get(ConnRef, "table:test", <<"a">>)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"b">>)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"c">>)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"d">>)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"e">>)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"f">>)),
                        ?assertMatch({ok, <<"gooseberry">>}, get(ConnRef, "table:test", <<"g">>))
                end},
               {"drop table",
                fun() ->
                        ?assertMatch(ok, drop(ConnRef, "table:test"))
                end}
              ]}
     end}.

various_maintenance_test_() ->
    {setup,
     fun () ->
             {ok, CWD} = file:get_cwd(),
             ?assertMatch(ok, filelib:ensure_dir(filename:join([?TEST_DATA_DIR, "x"]))),
             {ok, ConnRef} = connection_open(filename:join([CWD, ?TEST_DATA_DIR]), []),
             ConnRef
     end,
     fun (ConnRef) ->
             ?assertMatch(ok, connection_close(ConnRef))
     end,
     fun(ConnRef) ->
             {inorder,
              [
               {"drop table",
                fun() ->
                        ?assertMatch(ok, create(ConnRef, "table:test")),
                        ?assertMatch(ok, drop(ConnRef, "table:test")),
                        ?assertMatch(ok, create(ConnRef, "table:test"))
                end},
               {"salvage",
                fun() ->
                        ?assertMatch(ok, salvage(ConnRef, "table:test"))
                end},
               {"upgrade",
                fun() ->
                        ?assertMatch(ok, upgrade(ConnRef, "table:test"))
                end},
               {"rename",
                fun() ->
                        ?assertMatch(ok, rename(ConnRef, "table:test", "table:new")),
                        ?assertMatch(ok, rename(ConnRef, "table:new", "table:test"))
                end}
              ]}
     end}.

cursor_open_close_test() ->
    ConnRef = init_test_table(),
    {ok, Cursor1} = cursor_open(ConnRef, "table:test"),
    ?assertMatch({ok, <<"a">>, <<"apple">>}, cursor_next(Cursor1)),
    ?assertMatch(ok, cursor_close(Cursor1)),
    {ok, Cursor2} = cursor_open(ConnRef, "table:test"),
    ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, cursor_prev(Cursor2)),
    ?assertMatch(ok, cursor_close(Cursor2)),
    stop_test_table(ConnRef).

various_cursor_test_() ->
    {setup,
     fun init_test_table/0,
     fun stop_test_table/1,
     fun(ConnRef) ->
             {inorder,
              [{"move a cursor back and forth, getting key",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, <<"a">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"b">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"c">>}, cursor_prev_key(Cursor)),
                        ?assertMatch({ok, <<"d">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"e">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"f">>}, cursor_next_key(Cursor)),
                        ?assertMatch({ok, <<"g">>}, cursor_next_key(Cursor)),
                        ?assertMatch(not_found, cursor_next_key(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting value",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"banana">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"cherry">>}, cursor_prev_value(Cursor)),
                        ?assertMatch({ok, <<"date">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"elephant">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"forest">>}, cursor_next_value(Cursor)),
                        ?assertMatch({ok, <<"gooseberry">>}, cursor_next_value(Cursor)),
                        ?assertMatch(not_found, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"move a cursor back and forth, getting key and value",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, <<"a">>, <<"apple">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"b">>, <<"banana">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"c">>, <<"cherry">>}, cursor_prev(Cursor)),
                        ?assertMatch({ok, <<"d">>, <<"date">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"e">>, <<"elephant">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"f">>, <<"forest">>}, cursor_next(Cursor)),
                        ?assertMatch({ok, <<"g">>, <<"gooseberry">>}, cursor_next(Cursor)),
                        ?assertMatch(not_found, cursor_next(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"fold keys",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch([<<"g">>, <<"f">>, <<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
                                     fold_keys(Cursor, fun(Key, Acc) -> [Key | Acc] end, [])),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"search for an item",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, <<"banana">>}, cursor_search(Cursor, <<"b">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"proxmity search for an item, and find it",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, match}, cursor_search_near(Cursor, <<"e">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"proxmity search for an item, find next smallest key",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, lt}, cursor_search_near(Cursor, <<"z">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"proxmity search for an item, find next largest key",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, gt}, cursor_search_near(Cursor, <<"0">>)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"check cursor reset",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_reset(Cursor)),
                        ?assertMatch({ok, <<"apple">>}, cursor_next_value(Cursor)),
                        ?assertMatch(ok, cursor_close(Cursor))
                end},
               {"insert/overwrite an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch(ok, cursor_insert(Cursor, <<"h">>, <<"huckleberry">>)),
                        ?assertMatch({ok, <<"huckleberry">>}, cursor_search(Cursor, <<"h">>)),
                        ?assertMatch(ok, cursor_insert(Cursor, <<"g">>, <<"grapefruit">>)),
                        ?assertMatch({ok, <<"grapefruit">>}, cursor_search(Cursor, <<"g">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch({ok, <<"grapefruit">>}, get(ConnRef, "table:test", <<"g">>)),
                        ?assertMatch({ok, <<"huckleberry">>}, get(ConnRef, "table:test", <<"h">>))
                end},
               {"update an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch(ok, cursor_update(Cursor, <<"g">>, <<"goji berries">>)),
                        ?assertMatch(not_found, cursor_update(Cursor, <<"k">>, <<"kumquat">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch({ok, <<"goji berries">>}, get(ConnRef, "table:test", <<"g">>))
                end},
               {"remove an item using a cursor",
                fun() ->
                        {ok, Cursor} = cursor_open(ConnRef, "table:test"),
                        ?assertMatch(ok, cursor_remove(Cursor, <<"g">>)),
                        ?assertMatch(not_found, cursor_remove(Cursor, <<"l">>)),
                        ?assertMatch(ok, cursor_close(Cursor)),
                        ?assertMatch(not_found, get(ConnRef, "table:test", <<"g">>))
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

apply_kv_ops([], _ConnRef, _Tbl, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], ConnRef, Tbl, Acc0) ->
    ok = wterl:put(ConnRef, Tbl, K, V),
    apply_kv_ops(Rest, ConnRef, Tbl, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], ConnRef, Tbl, Acc0) ->
    ok = case wterl:delete(ConnRef, Tbl, K) of
             ok ->
                 ok;
             not_found ->
                 ok;
             Else ->
                 Else
         end,
    apply_kv_ops(Rest, ConnRef, Tbl, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     DataDir = "test/wterl.putdelete.qc",
                     Table = "table:eqc",
                     {ok, CWD} = file:get_cwd(),
                     ?cmd("rm -rf "++DataDir),
                     ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
                     {ok, ConnRef} = wterl:connection_open(DataDir, [{create,true}]),
                     try
                         wterl:create(ConnRef, Table),
                         Model = apply_kv_ops(Ops, ConnRef, Table, []),

                         %% Validate that all deleted values return not_found
                         F = fun({K, deleted}) ->
                                     ?assertEqual(not_found, wterl:get(ConnRef, Table, K));
                                ({K, V}) ->
                                     ?assertEqual({ok, V}, wterl:get(ConnRef, Table, K))
                             end,
                         lists:map(F, Model),
                         true
                     after
                         wterl:connection_close(ConnRef)
                     end
                 end)).

prop_put_delete_test_() ->
    {timeout, 3*60, fun() -> qc(prop_put_delete()) end}.

-endif.

-endif.
