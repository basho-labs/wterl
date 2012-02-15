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
         session_new/1,
         session_get/3,
         session_put/4,
         session_delete/3,
         session_close/1,
         table_create/2,
         table_create/3,
         table_drop/2,
         table_drop/3,
         cursor_create/2,
         cursor_close/1,
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

session_new(_ConnRef) ->
    ?nif_stub.

session_get(_Ref, _Table, _Key) ->
    ?nif_stub.

session_put(_Ref, _Table, _Key, _Value) ->
    ?nif_stub.

session_delete(_Ref, _Table, _Key) ->
    ?nif_stub.

session_close(_Ref) ->
    ?nif_stub.

table_create(Ref, Name) ->
    table_create(Ref, Name, "").

table_create(_Ref, _Name, _Config) ->
    ?nif_stub.

table_drop(Ref, Name) ->
    table_drop(Ref, Name, "").

table_drop(_Ref, _Name, _Config) ->
    ?nif_stub.

cursor_create(_Ref, _Table) ->
    ?nif_stub.

cursor_close(_Cursor) ->
    ?nif_stub.

%%
%% Configuration type information.
%%
config_types() ->
    [{cache_size, integer},
     {create, bool},
     {error_prefix, string},
     {eviction_target, integer},
     {eviction_trigger, integer},
     {exclusive, false},
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
    Opts = [{create, true}],
    ok = filelib:ensure_dir(filename:join("/tmp/wterl.basic", "foo")),
    {ok, ConnRef} = conn_open("/tmp/wterl.basic", config_to_bin(Opts, [])),
    {ok, SRef} = session_new(ConnRef),
    {ok, Table} = table_create(SRef, "table:test"),
    ok = session_put(SRef, Table, <<"a">>, <<"apple">>),
    {ok, <<"apple">>} = session_get(SRef, Table, <<"a">>),
    ok = session_delete(SRef, Table, <<"a">>),
    {error, not_found} = session_get(SRef, Table, <<"a">>),
    ok = session_close(SRef),
    ok = conn_close(ConnRef).

-endif.
