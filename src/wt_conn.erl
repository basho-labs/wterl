%% -------------------------------------------------------------------
%%
%% wt_conn: manage a connection to WiredTiger
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
-module(wt_conn).
-author('Steve Vinoski <steve@basho.com>').

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

%% API
-export([start_link/0, stop/0,
         open/1, open/2, is_open/0, get/0, close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          conn :: wt:connection()
         }).

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

-spec open(string()) -> {ok, wt:connection()} | {error, term()}.
open(Dir) ->
    open(Dir, []).

-spec open(string(), config_list()) -> {ok, wt:connection()} | {error, term()}.
open(Dir, Config) ->
    gen_server:call(?MODULE, {open, Dir, Config, self()}, infinity).

-spec is_open() -> boolean().
is_open() ->
    gen_server:call(?MODULE, is_open, infinity).

-spec get() -> {ok, reference()} | {error, term()}.
get() ->
    gen_server:call(?MODULE, get, infinity).

-spec close(wt:connection()) -> ok.
close(_Conn) ->
    gen_server:call(?MODULE, {close, self()}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    true = wt_conn_deputy:table_ready(),
    {ok, #state{}}.

handle_call({open, Dir, Config, Caller}, _From, #state{conn=undefined}=State) ->
    Opts = tailor_config(Config),
    {Reply, NState} =
	case wt:conn_open(Dir, wt:config_to_bin(Opts)) of
	    {ok, ConnRef}=OK ->
		Monitor = erlang:monitor(process, Caller),
		true = ets:insert(wt_conn_deputy, {Monitor, Caller}),
		{OK, State#state{conn = ConnRef}};
	    Error ->
		{Error, State}
	end,
    {reply, Reply, NState};
handle_call({open, _Dir, _Config, Caller}, _From,#state{conn=ConnRef}=State) ->
    Monitor = erlang:monitor(process, Caller),
    true = ets:insert(wt_conn_deputy, {Monitor, Caller}),
    {reply, {ok, ConnRef}, State};

handle_call(is_open, _From, #state{conn=ConnRef}=State) ->
    {reply, ConnRef /= undefined, State};

handle_call(get, _From, #state{conn=undefined}=State) ->
    {reply, {error, "no connection"}, State};
handle_call(get, _From, #state{conn=ConnRef}=State) ->
    {reply, {ok, ConnRef}, State};

handle_call({close, Caller}, _From, #state{conn=ConnRef}=State) ->
    {[{Monitor, Caller}], _} = ets:match_object(wt_conn_deputy, {'_', Caller}, 1),
    true = erlang:demonitor(Monitor, [flush]),
    true = ets:delete(wt_conn_deputy, Monitor),
    NState = case ets:info(wt_conn_deputy, size) of
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
                      ets:delete(wt_conn_deputy, Monitor)
              end, true, wt_conn_deputy),
    {stop, normal, State#state{conn=undefined}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Monitor, _, _, _}, #state{conn=ConnRef}=State) ->
    NState = case ets:lookup(wt_conn_deputy, Monitor) of
                 [{Monitor, _}] ->
                     true = ets:delete(wt_conn_deputy, Monitor),
                     case ets:info(wt_conn_deputy, size) of
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
    wt:conn_close(ConnRef).

%% @private
config_value(Key, Config, Default) ->
    {Key, app_helper:get_prop_or_env(Key, Config, wt, Default)}.

%% @private
map_cfg([], Acc) ->
    Acc;
map_cfg([Fun|T], Acc) ->
    map_cfg(T, Fun(Acc)).

tailor_config(Config) ->
    map_cfg([fun (Acc) ->
		     case proplists:is_defined(create, Acc) of
			 false -> [{create, true} | Acc];
			 true -> Acc
		     end
		 end,
	     fun (Acc) ->
		     case proplists:is_defined(shared_cache, Acc) of
			 false ->
			     [config_value(cache_size, Acc, "512MB") | Acc];
			 true ->
			     Acc
		     end
	     end,
	     fun (Acc) ->
		     case proplists:is_defined(session_max, Acc) of
			 false ->
			     [config_value(session_max, Acc, 100) | Acc];
			 true ->
			     Acc
		     end
	     end], Config).

-ifdef(TEST).

-define(DATADIR, "test/wt-backend").

simple_test_() ->
    {spawn,
     [{setup,
       fun() ->
               ?assertCmd("rm -rf " ++ ?DATADIR),
               ?assertMatch(ok, filelib:ensure_dir(filename:join(?DATADIR, "x"))),
               EtsPid = case wt_conn_deputy:start_link() of
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
               wt_conn_deputy:stop()
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
    {ok, Ref} = open("test/wt-backend", [{create, true},{session_max, 20}]),
    true = is_open(),
    close(Ref),
    false = is_open(),
    ok.

open_and_wait(Pid) ->
    {ok, Ref} = open("test/wt-backend", [{create, true}]),
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
