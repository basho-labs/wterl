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
         open/1, open/2, is_open/0, get/0, close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { conn :: wterl:connection() }).

-type config_list() :: [{atom(), any()}].

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
    open(Dir, []).

-spec open(string(), config_list()) -> {ok, wterl:connection()} | {error, term()}.
open(Dir, Config) ->
    gen_server:call(?MODULE, {open, Dir, Config, self()}, infinity).

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
    true = wterl_ets:table_ready(),
    {ok, #state{}}.

handle_call({open, Dir, Config, Caller}, _From, #state{conn=undefined}=State) ->
    {Reply, NState} = case wterl:conn_open(Dir, wterl:config_to_bin(Config)) of
                          {ok, ConnRef}=OK ->
                              Monitor = erlang:monitor(process, Caller),
                              true = ets:insert(wterl_ets, {Monitor, Caller}),
                              {OK, State#state{conn = ConnRef}};
                          Error ->
                              {Error, State}
                      end,
    {reply, Reply, NState};
handle_call({open, _Dir, _Config, Caller}, _From,#state{conn=ConnRef}=State) ->
    Monitor = erlang:monitor(process, Caller),
    true = ets:insert(wterl_ets, {Monitor, Caller}),
    {reply, {ok, ConnRef}, State};

handle_call(is_open, _From, #state{conn=ConnRef}=State) ->
    {reply, ConnRef /= undefined, State};

handle_call(get, _From, #state{conn=undefined}=State) ->
    {reply, {error, "no connection"}, State};
handle_call(get, _From, #state{conn=ConnRef}=State) ->
    {reply, {ok, ConnRef}, State};

handle_call({close, Caller}, _From, #state{conn=ConnRef}=State) ->
    {[{Monitor, Caller}], _} = ets:match_object(wterl_ets, {'_', Caller}, 1),
    true = erlang:demonitor(Monitor, [flush]),
    true = ets:delete(wterl_ets, Monitor),
    NState = case ets:info(wterl_ets, size) of
                 0 ->
                     do_close(ConnRef),
                     State#state{conn=undefined};
                 _ ->
                     State
             end,
    {reply, ok, NState};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, #state{conn=undefined}=State) ->
    {stop, normal, State};
handle_cast(stop, #state{conn=ConnRef}=State) ->
    do_close(ConnRef),
    ets:foldl(fun({Monitor, _}, _) ->
                      true = erl:demonitor(Monitor, [flush]),
                      ets:delete(wterl_ets, Monitor)
              end, true, wterl_ets),
    {stop, normal, State#state{conn=undefined}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Monitor, _, _, _}, #state{conn=ConnRef}=State) ->
    NState = case ets:lookup(wterl_ets, Monitor) of
                 [{Monitor, _}] ->
                     true = ets:delete(wterl_ets, Monitor),
                     case ets:info(wterl_ets, size) of
                         0 ->
                             do_close(ConnRef),
                             State#state{conn=undefined};
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

%% @private
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
               EtsPid = case wterl_ets:start_link() of
                            {ok, Pid1} ->
                                Pid1;
                            {error, {already_started, Pid1}} ->
                                Pid1
                        end,
               MyPid = case start_link() of
                           {ok, Pid2} ->
                               Pid2;
                           {error, {already_started, Pid2}} ->
                               Pid2
                       end,
               {EtsPid, MyPid}
       end,
       fun(_) ->
               stop(),
               wterl_ets:stop()
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
    {ok, Ref} = open("test/wterl-backend", [{create, true}, {session_max, 20},{cache_size, "1MB"}]),
    true = is_open(),
    close(Ref),
    false = is_open(),
    ok.

open_and_wait(Pid) ->
    {ok, Ref} = open("test/wterl-backend", [{create, true}]),
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
