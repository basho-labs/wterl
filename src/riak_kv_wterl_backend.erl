%% -------------------------------------------------------------------
%%
%% riak_kv_wterl_backend: WiredTiger Driver for Riak
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_wterl_backend).
-behavior(temp_riak_kv_backend).
-author('Steve Vinoski <steve@basho.com>').

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compiel(export_all).
-endif.

-define(API_VERSION, 1).
%% TODO: for when this backend supports 2i
%%-define(CAPABILITIES, [async_fold, indexes]).
-define(CAPABILITIES, [async_fold]).

-record(pass, {session :: wterl:session(),
	       cursor  :: wterl:cursor()}).
-type pass() :: #pass{}.

-record(state, {table :: string(),
                connection :: wterl:connection(),
		passes :: [pass()]}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the wterl backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    lager:start(),
    AppStart =
        case application:start(wterl) of
            ok ->
                ok;
            {error, {already_started, _}} ->
                ok;
        {error, Reason1} ->
            lager:error("Failed to start wterl: ~p", [Reason1]),
            {error, Reason1}
        end,
    case AppStart of
        ok ->
            Table = "lsm:wt" ++ integer_to_list(Partition),
            {ok, Connection} = establish_connection(Config),
	    Passes = establish_passes(erlang:system_info(schedulers), Connection, Table),
	    {ok, #state{table=Table, connection=Connection, passes=Passes}};
        {error, Reason2} ->
            {error, Reason2}
    end.

%% @doc Stop the wterl backend
-spec stop(state()) -> ok.
stop(#state{passes=Passes}) ->
    lists:foreach(fun({Session, Cursor}) ->
			  ok = wterl:cursor_close(Cursor),
			  ok = wterl:session_close(Session)
		  end, Passes),
    ok.

%% @doc Retrieve an object from the wterl backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{passes=Passes}=State) ->
    WTKey = to_object_key(Bucket, Key),
    {_Session, Cursor} = lists:nth(erlang:system_info(scheduler_id), Passes),
    case wterl:cursor_search(Cursor, WTKey) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the wterl backend.
%% NOTE: The wterl backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, #state{passes=Passes}=State) ->
    WTKey = to_object_key(Bucket, PrimaryKey),
    {_Session, Cursor} = lists:nth(erlang:system_info(scheduler_id), Passes),
    case wterl:cursor_insert(Cursor, WTKey, Val) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the wterl backend
%% NOTE: The wterl backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{passes=Passes}=State) ->
    WTKey = to_object_key(Bucket, Key),
    {_Session, Cursor} = lists:nth(erlang:system_info(scheduler_id), Passes),
    case wterl:cursor_remove(Cursor, WTKey) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{table=Table}) ->
    {ok, Connection} = wterl_conn:get(),
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    BucketFolder =
        fun() ->
                {ok, Session} = wterl:session_open(Connection),
                case wterl:cursor_open(Session, Table) of
                    {error, "No such file or directory"} ->
                        Acc;
                    {ok, Cursor} ->
                        try
                            {FoldResult, _} =
                                wterl:fold_keys(Cursor, FoldFun, {Acc, []}),
                            FoldResult
                        catch
                            {break, AccFinal} ->
                                AccFinal
                        after
                            ok = wterl:cursor_close(Cursor),
			    ok = wterl:session_close(Session)
                        end
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{table=Table}) ->
    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    Limiter =
        if Index /= false  -> Index;
           Bucket /= false -> Bucket;
           true            -> undefined
        end,

    {ok, Connection} = wterl_conn:get(),

    %% Set up the fold...
    FoldFun = fold_keys_fun(FoldKeysFun, Limiter),
    KeyFolder =
        fun() ->
                {ok, Session} = wterl:session_open(Connection),
                case wterl:cursor_open(Session, Table) of
                    {error, "No such file or directory"} ->
                        Acc;
                    {ok, Cursor} ->
                        try
                            wterl:fold_keys(Cursor, FoldFun, Acc)
                        catch
                            {break, AccFinal} ->
                                AccFinal
                        after
                            ok = wterl:cursor_close(Cursor),
			    ok = wterl:session_close(Session)
                        end
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{table=Table}) ->
    {ok, Connection} = wterl_conn:get(),
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    ObjectFolder =
        fun() ->
                {ok, Session} = wterl:session_open(Connection),
                case wterl:cursor_open(Session, Table) of
                    {error, "No such file or directory"} ->
                        Acc;
                    {ok, Cursor} ->
                        try
                            wterl:fold(Cursor, FoldFun, Acc)
                        catch
                            {break, AccFinal} ->
                                AccFinal
                        after
                            ok = wterl:cursor_close(Cursor),
			    ok = wterl:session_close(Session)
                        end
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @doc Delete all objects from this wterl backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{passes=Passes, table=Table}=State) ->
    {Session, _Cursor} = lists:nth(erlang:system_info(scheduler_id), Passes),
    case wterl:session_truncate(Session, Table) of
        ok ->
            {ok, State};
        Error ->
            {error, Error, State}
    end.

%% @doc Returns true if this wterl backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{passes=Passes}) ->
    {_Session, Cursor} = lists:nth(erlang:system_info(scheduler_id), Passes),
    wterl:cursor_reset(Cursor),
    try
        not_found =:= wterl:cursor_next(Cursor)
    after
        ok = wterl:cursor_close(Cursor)
    end.

%% @doc Get the status information for this wterl backend
-spec status(state()) -> [{atom(), term()}].
status(#state{passes=Passes}) ->
    {_Session, Cursor} = lists:nth(erlang:system_info(scheduler_id), Passes),
    try
        Stats = fetch_status(Cursor),
        [{stats, Stats}]
    after
        ok = wterl:cursor_close(Cursor)
    end.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
establish_connection(Config) ->
    %% Get the data root directory
    case app_helper:get_prop_or_env(data_root, Config, wterl) of
        undefined ->
            lager:error("Failed to create wterl dir: data_root is not set"),
            {error, data_root_unset};
        DataRoot ->
            ok = filelib:ensure_dir(filename:join(DataRoot, "x")),
            SessionMax =
                case app_helper:get_prop_or_env(ring_creation_size, Config, riak_core) of
                    undefined -> 1024;
                    RingSize when RingSize < 512 -> 1024;
                    RingSize -> RingSize * 2
                end,
	    RequestedCacheSize = app_helper:get_prop_or_env(cache_size, Config, wterl),
            Opts = orddict:from_list(
                     [ wterl:config_value(create, Config, true),
                       wterl:config_value(sync, Config, false),
                       wterl:config_value(logging, Config, true),
                       wterl:config_value(transactional, Config, true),
                       wterl:config_value(session_max, Config, SessionMax),
		       wterl:config_value(cache_size, Config, size_cache(RequestedCacheSize)),
                       wterl:config_value(statistics_log, Config, [{wait, 30}]), % sec
		       wterl:config_value(verbose, Config, [ 
			  %"ckpt" "block", "shared_cache", "evictserver", "fileops",
			  %"hazard", "mutex", "read", "readserver", "reconcile",
			  %"salvage", "verify", "write", "evict", "lsm"
			])
		     ] ++ proplists:get_value(wterl, Config, [])), % sec
            lager:info("WiredTiger connection:open(~s, ~s)", [DataRoot, wterl:config_to_bin(Opts)]),
            case wterl_conn:open(DataRoot, Opts) of
                {ok, Connection} ->
                    {ok, Connection};
                {error, Reason2} ->
		    lager:error("Failed to establish a WiredTiger connection, wterl backend unable to start: ~p\n", [Reason2]),
                    {error, Reason2}
            end
    end.

establish_passes(Count, Connection, Table)
  when is_number(Count), Count > 0 ->
    establish_passes(Count, Connection, Table, []).

establish_passes(Count, Connection, Table, Acc)
  when Count > 0 ->
    case Count > 1 of
	true ->
	    {ok, Session} = establish_session(Connection, Table),
	    {ok, Cursor} = wterl:cursor_open(Session, Table),
	    [{Session, Cursor} | establish_passes(Count - 1, Connection, Table, Acc)];
	false ->
	    Acc
    end.

%% @private
establish_session(Connection, Table) ->
    case wterl:session_open(Connection, wterl:config_to_bin([{isolation, "snapshot"}])) of
        {ok, Session} ->
            SessionOpts =
                [%TODO {block_compressor, "snappy"},
                 {internal_page_max, "128K"},
                 {leaf_page_max, "128K"},
                 {lsm_chunk_size, "200MB"},
		 {lsm_bloom_newest, true},
		 {lsm_bloom_oldest, true} ,
                 {lsm_bloom_config, [{leaf_page_max, "10MB"}]} ],
            case wterl:session_create(Session, Table, wterl:config_to_bin(SessionOpts)) of
                ok ->
		    {ok, Session};
                {error, Reason} ->
                    lager:error("Failed to start wterl backend: ~p\n", [Reason]),
		    {error, Reason}
            end;
        {error, Reason} ->
            lager:error("Failed to open a WiredTiger session: ~p\n", [Reason]),
	    {error, Reason}
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(BK, {Acc, LastBucket}) ->
            case from_object_key(BK) of
                {LastBucket, _} ->
                    {Acc, LastBucket};
                {Bucket, _} ->
                    {FoldBucketsFun(Bucket, Acc), Bucket};
                _ ->
                    throw({break, Acc})
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    %% Fold across everything...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} ->
                    FoldKeysFun(Bucket, Key, Acc);
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {bucket, FilterBucket}) ->
    %% Fold across a specific bucket...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when Bucket == FilterBucket ->
                    FoldKeysFun(Bucket, Key, Acc);
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, <<"$bucket">>, _}}) ->
    %% 2I exact match query on special $bucket field...
    fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, FilterField, FilterTerm}}) ->
    %% Rewrite 2I exact match query as a range...
    NewQuery = {range, FilterField, FilterTerm, FilterTerm},
    fold_keys_fun(FoldKeysFun, {index, FilterBucket, NewQuery});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, <<"$key">>, StartKey, EndKey}}) ->
    %% 2I range query on special $key field...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when FilterBucket == Bucket,
                                   StartKey =< Key,
                                   EndKey >= Key ->
                    FoldKeysFun(Bucket, Key, Acc);
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, FilterField, StartTerm, EndTerm}}) ->
    %% 2I range query...
    fun(StorageKey, Acc) ->
            case from_index_key(StorageKey) of
                {Bucket, Key, Field, Term} when FilterBucket == Bucket,
                                                FilterField == Field,
                                                StartTerm =< Term,
                                                EndTerm >= Term ->
                    FoldKeysFun(Bucket, Key, Acc);
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(_FoldKeysFun, Other) ->
    throw({unknown_limiter, Other}).

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, FilterBucket) ->
    %% 2I does not support fold objects at this time, so this is much
    %% simpler than fold_keys_fun.
    fun({StorageKey, Value}, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when FilterBucket == undefined;
                                   Bucket == FilterBucket ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                _ ->
                    throw({break, Acc})
            end
    end.

to_object_key(Bucket, Key) ->
    sext:encode({o, Bucket, Key}).

from_object_key(LKey) ->
    case sext:decode(LKey) of
        {o, Bucket, Key} ->
            {Bucket, Key};
        _ ->
            undefined
    end.

from_index_key(LKey) ->
    case sext:decode(LKey) of
        {i, Bucket, Field, Term, Key} ->
            {Bucket, Key, Field, Term};
        _ ->
            undefined
    end.

%% @private
%% Return all status from wterl statistics cursor
fetch_status(Cursor) ->
    fetch_status(Cursor, wterl:cursor_next_value(Cursor), []).
fetch_status(_Cursor, not_found, Acc) ->
    lists:reverse(Acc);
fetch_status(Cursor, {ok, Stat}, Acc) ->
    [What,Val|_] = [binary_to_list(B) || B <- binary:split(Stat, [<<0>>], [global])],
    fetch_status(Cursor, wterl:cursor_next_value(Cursor), [{What,Val}|Acc]).

size_cache(RequestedSize) ->
    Size =
        case RequestedSize of
            undefined ->
                RunningApps = application:which_applications(),
                FinalGuess =
                    case proplists:is_defined(sasl, RunningApps) andalso
                        proplists:is_defined(os_mon, RunningApps) of
                        true ->
                            Memory = memsup:get_system_memory_data(),
                            TotalRAM = proplists:get_value(system_total_memory, Memory),
                            FreeRAM = proplists:get_value(free_memory, Memory),
                            UsedByBeam = proplists:get_value(total, erlang:memory()),
                            Target = ((TotalRAM - UsedByBeam) div 4),
                            FirstGuess = (Target - (Target rem (1024 * 1024))),
                            SecondGuess =
                                case FirstGuess > FreeRAM of
                                    true -> FreeRAM - (FreeRAM rem (1024 * 1024));
                                    _ -> FirstGuess
                                end,
                            case SecondGuess < 1073741824 of %% < 1GB?
                                true -> "1GB";
                                false ->
                                    ThirdGuess = SecondGuess div (1024 * 1024),
                                    integer_to_list(ThirdGuess) ++ "MB"
                            end;
                        false ->
                            "1GB"
                    end,
                application:set_env(wterl, cache_size, FinalGuess),
		lager:info("Using cache size of ~p for WiredTiger storage backend.", [FinalGuess]),
                FinalGuess;
            Value when is_list(Value) ->
                Value;
            Value when is_number(Value) ->
                integer_to_list(Value)
        end,
    Size.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    ?assertCmd("rm -rf test/wterl-backend"),
    application:set_env(wterl, data_root, "test/wterl-backend"),
    temp_riak_kv_backend:standard_test(?MODULE, []).

custom_config_test_() ->
    ?assertCmd("rm -rf test/wterl-backend"),
    application:set_env(wterl, data_root, ""),
    temp_riak_kv_backend:standard_test(?MODULE, [{data_root, "test/wterl-backend"}]).

-endif.
