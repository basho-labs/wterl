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
-module(wterl_event_handler).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PREFIX, "wiredtiger").

%% ====================================================================
%% API
%% ====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:cast(?MODULE, stop).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    wterl:set_event_handler_pid(self()),
    {ok, []}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({error, {Errno, Message}}, State) ->
    log(error, "~s: (~s) ~s", [?PREFIX, Errno, Message]),
    {noreply, State};
handle_info({message, Info}, State) ->
    log(info, "~s: ~s", [?PREFIX, Info]),
    {noreply, State};
handle_info({progress, {Operation, Counter}}, State) ->
    log(info, "~s: progress on ~s [~b]", [?PREFIX, Operation, Counter]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
-spec log(error | info, string(), [any()]) -> ok.
log(Urgency, Format, Args) ->
    case proplists:is_defined(lager, application:which_applications()) of
        true ->
            log(lager, Urgency, Format, Args);
        false ->
            log(stdio, Urgency, Format, Args)
    end.

-spec log(lager | stdio, error | info, string(), [any()]) -> ok.
log(lager, error, Format, Args) ->
    lager:error(Format, Args);
log(lager, info, Format, Args) ->
    lager:info(Format, Args);
log(stdio, _, Format, Args) ->
    io:format(Format ++ "~n", Args).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-endif.
