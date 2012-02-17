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
         cursor_open/2,
         cursor_prev/1,
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
         config_to_bin/2]).

-on_load(init/0).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, bad_name} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    erlang:load_nif(filename:join(PrivDir, ?MODULE), 0).

conn_open(_HomeDir, _Config) ->
    ?nif_stub.

conn_close(_ConnRef) ->
    ?nif_stub.

session_open(_ConnRef) ->
    ?nif_stub.

session_close(_Ref) ->
    ?nif_stub.

session_create(Ref, Name) ->
    session_create(Ref, Name, "").
session_create(_Ref, _Name, _Config) ->
    ?nif_stub.

session_drop(Ref, Name) ->
    session_drop(Ref, Name, "").
session_drop(_Ref, _Name, _Config) ->
    ?nif_stub.

session_delete(_Ref, _Table, _Key) ->
    ?nif_stub.

session_get(_Ref, _Table, _Key) ->
    ?nif_stub.

session_put(_Ref, _Table, _Key, _Value) ->
    ?nif_stub.

session_rename(Ref, OldName, NewName) ->
    session_rename(Ref, OldName, NewName, "").
session_rename(_Ref, _OldName, _NewName, _Config) ->
    ?nif_stub.

session_salvage(Ref, Name) ->
    session_salvage(Ref, Name, "").
session_salvage(_Ref, _Name, _Config) ->
    ?nif_stub.

session_sync(Ref, Name) ->
    session_sync(Ref, Name, "").
session_sync(_Ref, _Name, _Config) ->
    ?nif_stub.

session_truncate(Ref, Name) ->
    session_truncate(Ref, Name, "").
session_truncate(_Ref, _Name, _Config) ->
    ?nif_stub.

session_upgrade(Ref, Name) ->
    session_upgrade(Ref, Name, "").
session_upgrade(_Ref, _Name, _Config) ->
    ?nif_stub.

session_verify(Ref, Name) ->
    session_verify(Ref, Name, "").
session_verify(_Ref, _Name, _Config) ->
    ?nif_stub.

cursor_open(_Ref, _Table) ->
    ?nif_stub.

cursor_close(_Cursor) ->
    ?nif_stub.

cursor_next(_Cursor) ->
    ?nif_stub.

cursor_prev(_Cursor) ->
    ?nif_stub.

cursor_search(_Cursor, _Key) ->
    ?nif_stub.

cursor_search_near(_Cursor, _Key) ->
    ?nif_stub.

cursor_reset(_Cursor) ->
    ?nif_stub.

cursor_insert(_Ref, _Key, _Value) ->
    ?nif_stub.

cursor_update(_Ref, _Key, _Value) ->
    ?nif_stub.

cursor_remove(_Ref, _Key, _Value) ->
    ?nif_stub.

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
    catch _:_ ->
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

config_to_bin([], Acc) ->
    iolist_to_binary([Acc, <<"\0">>]);
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
                           [EncodedKey, <<" = ">>, EncodedValue, <<", ">> | Acc]
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

basic_test() ->
    %% Open a connection and a session, create the test table.
    ok = filelib:ensure_dir(filename:join("/tmp/wterl.basic", "foo")),

    io:put_chars(standard_error, "\topen connection\n"),
    {ok, ConnRef} =
        conn_open("/tmp/wterl.basic", "create=true,cache_size=100MB"),
    {ok, SRef} = session_open(ConnRef),

    %% Remove the table from any earlier run and re-create it
    io:put_chars(standard_error, "\ttable drop/create\n"),
    ok = session_drop(SRef, "table:test", "force"),
    ok = session_create(SRef, "table:test"),

    %% Insert/delete a key using the session handle
    io:put_chars(standard_error, "\tsession insert/delete\n"),
    ok = session_put(SRef, "table:test", <<"a">>, <<"apple">>),
    {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),
    ok = session_delete(SRef, "table:test", <<"a">>),
    {error, not_found} = session_get(SRef, "table:test", <<"a">>),

    %% Insert some items
    io:put_chars(standard_error, "\tsession insert\n"),
    ok = session_put(SRef, "table:test", <<"a">>, <<"apple">>),
    ok = session_put(SRef, "table:test", <<"b">>, <<"banana">>),
    ok = session_put(SRef, "table:test", <<"c">>, <<"cherry">>),
    ok = session_put(SRef, "table:test", <<"d">>, <<"date">>),
    ok = session_put(SRef, "table:test", <<"g">>, <<"gooseberry">>),

    %% Remaining session operations.
    io:put_chars(standard_error, "\tsession verify\n"),
    ok = session_verify(SRef, "table:test"),
    {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),

    io:put_chars(standard_error, "\tsession sync\n"),
    ok = session_sync(SRef, "table:test"),
    {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),

    %% ===================================================================
    %% KEITH: SKIP SALVAGE FOR NOW, THERE IS SOMETHING WRONG.
    %% ===================================================================
    %% io:put_chars(standard_error, "\tsession salvage\n"),
    %% ok = session_salvage(SRef, "table:test"),
    %% {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),

    io:put_chars(standard_error, "\tsession truncate\n"),
    ok = session_truncate(SRef, "table:test"),
    {error, not_found} = session_get(SRef, "table:test", <<"a">>),

    ok = session_put(SRef, "table:test", <<"a">>, <<"apple">>),
    ok = session_put(SRef, "table:test", <<"b">>, <<"banana">>),
    ok = session_put(SRef, "table:test", <<"c">>, <<"cherry">>),
    ok = session_put(SRef, "table:test", <<"d">>, <<"date">>),
    ok = session_put(SRef, "table:test", <<"g">>, <<"gooseberry">>),

    io:put_chars(standard_error, "\tsession upgrade\n"),
    ok = session_upgrade(SRef, "table:test"),
    {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),

    io:put_chars(standard_error, "\tsession rename\n"),
    ok = session_rename(SRef, "table:test", "table:new"),
    {ok, <<"apple">>} = session_get(SRef, "table:new", <<"a">>),
    ok = session_rename(SRef, "table:new", "table:test"),
    {ok, <<"apple">>} = session_get(SRef, "table:test", <<"a">>),

    %% Open/close a pair of cursors, check first/last returns
    io:put_chars(standard_error, "\tcursor open/close, first/last\n"),
    {ok, Cursor1} = cursor_open(SRef, "table:test"),
    {ok, <<"apple">>} = cursor_next(Cursor1),
    ok = cursor_close(Cursor1),
    {ok, Cursor2} = cursor_open(SRef, "table:test"),
    {ok, <<"gooseberry">>} = cursor_prev(Cursor2),
    ok = cursor_close(Cursor2),

    %% Move a cursor back and forth
    io:put_chars(standard_error, "\tcursor next/prev\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    {ok, <<"apple">>} = cursor_next(Cursor),
    {ok, <<"banana">>} = cursor_next(Cursor),
    {ok, <<"cherry">>} = cursor_next(Cursor),
    {ok, <<"date">>} = cursor_next(Cursor),
    {ok, <<"cherry">>} = cursor_prev(Cursor),
    {ok, <<"date">>} = cursor_next(Cursor),
    {ok, <<"gooseberry">>} = cursor_next(Cursor),
    {error, not_found} = cursor_next(Cursor),
    ok = cursor_close(Cursor),

    %% Search for an item
    io:put_chars(standard_error, "\tcursor search\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    {ok, <<"banana">>} = cursor_search(Cursor, <<"b">>),
    ok = cursor_close(Cursor),

    %% Range search for an item
    io:put_chars(standard_error, "\tcursor search-near\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    {ok, <<"gooseberry">>} = cursor_search_near(Cursor, <<"z">>),
    ok = cursor_close(Cursor),

    %% Check that cursor reset works
    io:put_chars(standard_error, "\tcursor reset\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    {ok, <<"apple">>} = cursor_next(Cursor),
    ok = cursor_reset(Cursor),
    {ok, <<"apple">>} = cursor_next(Cursor),
    ok = cursor_close(Cursor),

    %% Insert/overwrite an item using a cursor
    io:put_chars(standard_error, "\tcursor insert/overwrite\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    ok = cursor_insert(Cursor, <<"h">>, <<"huckleberry">>),
    {ok, <<"huckleberry">>} = cursor_search(Cursor, <<"h">>),
    ok = cursor_insert(Cursor, <<"g">>, <<"grapefruit">>),
    {ok, <<"grapefruit">>} = cursor_search(Cursor, <<"g">>),
    ok = cursor_close(Cursor),
    {ok, <<"grapefruit">>} = session_get(SRef, "table:test", <<"g">>),
    {ok, <<"huckleberry">>} = session_get(SRef, "table:test", <<"h">>),

    %% Update an item using a cursor
    io:put_chars(standard_error, "\tcursor update\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    ok = cursor_update(Cursor, <<"g">>, <<"goji berries">>),
    {error, not_found} = cursor_update(Cursor, <<"k">>, <<"kumquat">>),
    ok = cursor_close(Cursor),
    {ok, <<"goji berries">>} = session_get(SRef, "table:test", <<"g">>),

    %% Remove an item using a cursor
    io:put_chars(standard_error, "\tcursor remove\n"),
    {ok, Cursor} = cursor_open(SRef, "table:test"),
    ok = cursor_remove(Cursor, <<"g">>, <<"goji berries">>),
    {error, not_found} = cursor_remove(Cursor, <<"l">>, <<"lemon">>),
    ok = cursor_close(Cursor),
    {error, not_found} = session_get(SRef, "table:test", <<"g">>),

    %% Close the session/connection
    io:put_chars(standard_error, "\tsession/connection close\n"),
    ok = session_close(SRef),
    ok = conn_close(ConnRef).

-endif.
