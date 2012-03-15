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
-export([start_link/0, stop/0,
         open/1, is_open/0, get/0, close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {conn :: wterl:connection(),
                monitors :: ets:tid()}).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:cast(?MODULE, stop).

-spec open(string()) -> {ok, wterl:connection()} | {error, term()}.
open(Dir) ->
    gen_server:call(?MODULE, {open, Dir, self()}, infinity).

-spec is_open() -> boolean().
is_open() ->
    gen_server:call(?MODULE, is_open, infinity).

-spec get() -> {ok, reference()} | {error, term()}.
get() ->
    gen_server:call(?MODULE, get, infinity).

-spec close(wterl:connection()) -> ok.
close(_Conn) ->
    gen_server:call(?MODULE, {close, self()}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({open, Dir, Caller}, _From, #state{conn=undefined}=State) ->
    Opts = [{create, true}, {cache_size, "100MB"}, {session_max, 100}],
    {Reply, NState} = case wterl:conn_open(Dir, wterl:config_to_bin(Opts)) of
                          {ok, ConnRef}=OK ->
                              Monitors = ets:new(?MODULE, []),
                              Monitor = erlang:monitor(process, Caller),
                              true = ets:insert(Monitors, {Monitor, Caller}),
                              {OK, State#state{conn = ConnRef, monitors=Monitors}};
                          Error ->
                              {Error, State}
                      end,
    {reply, Reply, NState};
handle_call({open, _Dir, Caller}, _From,#state{conn=ConnRef, monitors=Monitors}=State) ->
    Monitor = erlang:monitor(process, Caller),
    true = ets:insert(Monitors, {Monitor, Caller}),
    {reply, {ok, ConnRef}, State};

handle_call(is_open, _From, #state{conn=ConnRef}=State) ->
    {reply, ConnRef /= undefined, State};

handle_call(get, _From, #state{conn=undefined}=State) ->
    {reply, {error, "no connection"}, State};
handle_call(get, _From, #state{conn=ConnRef}=State) ->
    {reply, {ok, ConnRef}, State};

handle_call({close, Caller}, _From, #state{conn=ConnRef, monitors=Monitors}=State) ->
    {[{Monitor, Caller}], _} = ets:match_object(Monitors, {'_', Caller}, 1),
    true = erlang:demonitor(Monitor, [flush]),
    true = ets:delete(Monitors, Monitor),
    NState = case ets:info(Monitors, size) of
                 0 ->
                     do_close(ConnRef),
                     ets:delete(Monitors),
                     State#state{conn=undefined, monitors=undefined};
                 _ ->
                     State
             end,
    {reply, ok, NState}.

handle_cast(stop, #state{conn=undefined}=State) ->
    {noreply, State};
handle_cast(stop, #state{conn=ConnRef, monitors=Monitors}=State) ->
    do_close(ConnRef),
    ets:foldl(fun({Monitor, _}, _) ->
                      true = erl:demonitor(Monitor, [flush])
              end, true, Monitors),
    {noreply, State#state{conn=undefined, monitors=undefined}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Monitor, _, _, _}, #state{conn=ConnRef, monitors=Monitors}=State) ->
    NState = case ets:lookup(Monitors, Monitor) of
                 [{Monitor, _}] ->
                     true = ets:delete(Monitors, Monitor),
                     case ets:info(Monitors, size) of
                         0 ->
                             do_close(ConnRef),
                             ets:delete(Monitors),
                             State#state{conn=undefined, monitors=undefined};
                         _ ->
                             State
                     end;
                 _ ->
                     State
             end,
    {noreply, NState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn = ConnRef}) ->
    do_close(ConnRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

do_close(undefined) ->
    ok;
do_close(ConnRef) ->
    wterl:conn_close(ConnRef).


-ifdef(TEST).

-define(DATADIR, "test/wterl-backend").

simple_test_() ->
    {spawn,
     [{setup,
       fun() ->
               ?assertCmd("rm -rf " ++ ?DATADIR),
               ?assertMatch(ok, filelib:ensure_dir(filename:join(?DATADIR, "x"))),
               {ok, Pid} = start_link(),
               Pid
       end,
       fun(_) ->
               stop()
       end,
       fun(_) ->
               {inorder,
                [{"open one connection",
                  fun open_one/0},
                 {"open two connections",
                  fun open_two/0},
                 {"open two connections, kill one",
                  fun kill_one/0}
                ]}
       end}]}.

open_one() ->
    {ok, Ref} = open("test/wterl-backend"),
    true = is_open(),
    close(Ref),
    false = is_open(),
    ok.

open_and_wait(Pid) ->
    {ok, Ref} = open("test/wterl-backend"),
    Pid ! open,
    receive
        close ->
            close(Ref),
            Pid ! close;
        exit ->
            exit(normal)
    end.

open_two() ->
    Self = self(),
    Pid1 = spawn_link(fun() -> open_and_wait(Self) end),
    receive
        open ->
            true = is_open()
    end,
    Pid2 = spawn_link(fun() -> open_and_wait(Self) end),
    receive
        open ->
            true = is_open()
    end,
    Pid1 ! close,
    receive
        close ->
            true = is_open()
    end,
    Pid2 ! close,
    receive
        close ->
            false = is_open()
    end,
    ok.

kill_one() ->
    Self = self(),
    Pid1 = spawn_link(fun() -> open_and_wait(Self) end),
    receive
        open ->
            true = is_open()
    end,
    Pid2 = spawn_link(fun() -> open_and_wait(Self) end),
    receive
        open ->
            true = is_open()
    end,
    Pid1 ! exit,
    timer:sleep(100),
    true = is_open(),
    Pid2 ! close,
    receive
        close ->
            false = is_open()
    end,
    ok.

-endif.
