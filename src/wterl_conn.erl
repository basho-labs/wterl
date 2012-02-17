%% -------------------------------------------------------------------
%%
%% wterl_conn: manage a connection to WiredTiger
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
-module(wterl_conn).
-author('Steve Vinoski <steve@basho.com>').

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0,
         open/1, get/0, close/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { conn }).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

open(Dir) ->
    gen_server:call(?MODULE, {open, Dir}, infinity).

get() ->
    gen_server:call(?MODULE, get, infinity).

close() ->
    gen_server:call(?MODULE, close, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({open, Dir}, _From, #state{conn = undefined}=State) ->
    Opts = [{create, true}, {cache_size, "100MB"}],
    {Reply, NState} = case wterl:conn_open(Dir, wterl:config_to_bin(Opts)) of
                          {ok, ConnRef}=OK ->
                              {OK, State#state{conn = ConnRef}};
                          Error ->
                              {Error, State}
                      end,
    {reply, Reply, NState};
handle_call({open, _Dir}, _From,#state{conn = ConnRef}=State) ->
    {reply, {ok, ConnRef}, State};

handle_call(get, _From, #state{conn = undefined}=State) ->
    {reply, {error, "no connection"}, State};
handle_call(get, _From, #state{conn = ConnRef}=State) ->
    {reply, {ok, ConnRef}, State};

handle_call(close, _From, #state{conn = ConnRef}=State) ->
    close(ConnRef),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn = ConnRef}) ->
    close(ConnRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

close(undefined) ->
    ok;
close(ConnRef) ->
    wterl:conn_close(ConnRef).


-ifdef(TEST).


-endif.
