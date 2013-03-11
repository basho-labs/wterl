%% -------------------------------------------------------------------
%%
%% wt_conn_deputy: ets table owner for wt_conn
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
-module(wt_conn_deputy).
-author('Steve Vinoski <steve@basho.com>').

-behaviour(gen_server).

%% ====================================================================
%% The sole purpose of this module is to own the ets table used by the
%% wt_conn module. Holding the ets table in an otherwise do-nothing
%% server avoids losing the table and its contents should an unexpected
%% error occur in wt_conn if it were the owner instead. This module
%% is unit-tested as part of the wt_conn module.
%% ====================================================================

%% API
-export([start_link/0, stop/0,
         table_ready/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          monitors :: ets:tid()
         }).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec stop() -> ok.
stop() ->
    gen_server:cast(?MODULE, stop).

-spec table_ready() -> boolean().
table_ready() ->
    gen_server:call(?MODULE, table_ready).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    Monitors = ets:new(?MODULE, [named_table,public]),
    {ok, #state{monitors = Monitors}}.

handle_call(table_ready, _From, State) ->
    %% Always return true since the table is prepared in init.
    {reply, true, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{monitors=Monitors}) ->
    ets:delete(Monitors),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
