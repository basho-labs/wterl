%% -------------------------------------------------------------------
%%
%% riak_kv_wterl_backend: WiredTiger Driver for Riak
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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
-behavior(riak_kv_backend).
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
-endif.

-define(API_VERSION, 1).
%% TODO: for when this backend supports 2i
%%-define(CAPABILITIES, [async_fold, indexes]).
-define(CAPABILITIES, [async_fold]).

-record(state, {ref :: reference(),
                table :: string(),
                partition :: integer()}).

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
    %% Get the data root directory
    case app_helper:get_prop_or_env(data_root, Config, wterl) of
        undefined ->
            lager:error("Failed to create wterl dir: data_root is not set"),
            {error, data_root_unset};
        DataRoot ->
            ok = filelib:ensure_dir(filename:join(DataRoot, "x")),
            case wterl_conn:open(DataRoot) of
                {ok, ConnRef} ->
                    Table = "table:wt" ++ integer_to_list(Partition),
                    {ok, SRef} = wterl:session_open(ConnRef),
                    try
                        ok = wterl:session_create(SRef, Table)
                    after
                        ok = wterl:session_close(SRef)
                    end,
                    {ok, #state{ref=ConnRef,
                                table=Table,
                                partition=Partition}};
                {error, Reason} ->
                    lager:error("Failed to start wterl backend: ~p\n",
                                [Reason]),
                    {error, Reason}
            end
    end.

%% @doc Stop the wterl backend
-spec stop(state()) -> ok.
stop(_State) ->
    wterl_conn:close().

%% @doc Retrieve an object from the wterl backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{ref=Ref, table=Table}=State) ->
    WTKey = to_object_key(Bucket, Key),
    {ok, SRef} = wterl:session_open(Ref),
    try
        case wterl:session_get(SRef, Table, WTKey) of
            {ok, Value} ->
                {ok, Value, State};
            not_found  ->
                {error, not_found, State};
            {error, Reason} ->
                {error, Reason, State}
        end
    after
        ok = wterl:session_close(SRef)
    end.

%% @doc Insert an object into the wterl backend.
%% NOTE: The wterl backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, #state{ref=Ref, table=Table}=State) ->
    WTKey = to_object_key(Bucket, PrimaryKey),
    {ok, SRef} = wterl:session_open(Ref),
    try
        case wterl:session_put(SRef, Table, WTKey, Val) of
            ok ->
                {ok, State};
            {error, Reason} ->
                {error, Reason, State}
        end
    after
        ok = wterl:session_close(SRef)
    end.

%% @doc Delete an object from the wterl backend
%% NOTE: The wterl backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{ref=Ref, table=Table}=State) ->
    WTKey = to_object_key(Bucket, Key),
    {ok, SRef} = wterl:session_open(Ref),
    try
        case wterl:session_delete(SRef, Table, WTKey) of
            ok ->
                {ok, State};
            {error, Reason} ->
                {error, Reason, State}
        end
    after
        ok = wterl:session_close(SRef)
    end.


%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{ref=Ref, table=Table}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    BucketFolder =
        fun() ->
                {ok, SRef} = wterl:session_open(Ref),
                {ok, Cursor} = wterl:cursor_open(SRef, Table),
                try
                    {FoldResult, _} =
                        wterl:fold_keys(Cursor, FoldFun, {Acc, []}),
                    FoldResult
                catch
                    {break, AccFinal} ->
                        AccFinal
                after
                    ok = wterl:cursor_close(Cursor),
                    ok = wterl:session_close(SRef)
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
fold_keys(FoldKeysFun, Acc, Opts, #state{ref=Ref, table=Table}) ->
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

    %% Set up the fold...
    FoldFun = fold_keys_fun(FoldKeysFun, Limiter),
    KeyFolder =
        fun() ->
                {ok, SRef} = wterl:session_open(Ref),
                {ok, Cursor} = wterl:cursor_open(SRef, Table),
                try
                    wterl:fold_keys(Cursor, FoldFun, Acc)
                catch
                    {break, AccFinal} ->
                        AccFinal
                after
                    ok = wterl:cursor_close(Cursor),
                    ok = wterl:session_close(SRef)
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
fold_objects(FoldObjectsFun, Acc, Opts, #state{ref=Ref, table=Table}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    ObjectFolder =
        fun() ->
                {ok, SRef} = wterl:session_open(Ref),
                {ok, Cursor} = wterl:cursor_open(SRef, Table),
                try
                    wterl:fold(Cursor, FoldFun, Acc)
                catch
                    {break, AccFinal} ->
                        AccFinal
                after
                    ok = wterl:cursor_close(Cursor),
                    ok = wterl:session_close(SRef)
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
drop(#state{ref=Ref, table=Table}=State) ->
    {ok, SRef} = wterl:session_open(Ref),
    try
        ok = wterl:session_truncate(SRef, Table),
        {ok, State}
    after
        ok = wterl:session_close(SRef)
    end.


%% @doc Returns true if this wterl backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{ref=Ref, table=Table}) ->
    {ok, SRef} = wterl:session_open(Ref),
    {ok, Cursor} = wterl:cursor_open(SRef, Table),
    try
        not_found =:= wterl:cursor_next(Cursor)
    after
        ok = wterl:cursor_close(Cursor),
        ok = wterl:session_close(SRef)
    end.

%% @doc Get the status information for this wterl backend
-spec status(state()) -> [{atom(), term()}].
status(#state{ref=Ref, table=Table}) ->
    {ok, SRef} = wterl:session_open(Ref),
    {ok, Cursor} = wterl:cursor_open(SRef, "statistics:"++Table),
    try
        Stats = fetch_status(Cursor),
        [{stats, Stats}]
    after
        ok = wterl:cursor_close(Cursor),
        ok = wterl:session_close(SRef)
    end.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

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
    fetch_status(Cursor, wterl:cursor_next(Cursor), []).
fetch_status(_Cursor, not_found, Acc) ->
    lists:reverse(Acc);
fetch_status(Cursor, {ok, Stat}, Acc) ->
    fetch_status(Cursor, wterl:cursor_next(Cursor), [Stat|Acc]).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    {setup,
     fun() ->
             ?assertCmd("rm -rf test/wterl-backend"),
             application:set_env(wterl, data_root, "test/wterl-backend"),
             application:start(wterl),
             {ok, S} = ?MODULE:start(42, []),
             S
     end,
     fun(S) ->
             ?MODULE:stop(S),
             application:stop(wterl)
     end,
     fun(State) ->
             [{"basic store and fetch test",
               fun() ->
                       [
                        ?assertMatch({ok, _},
                                     ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, State)),
                        ?assertMatch({ok, _},
                                     ?MODULE:put(<<"b2">>, <<"k2">>, [], <<"v2">>, State)),
                        ?assertMatch({ok,<<"v2">>, _},
                                     ?MODULE:get(<<"b2">>, <<"k2">>, State)),
                        ?assertMatch({error, not_found, _},
                                     ?MODULE:get(<<"b1">>, <<"k3">>, State))
                       ]
               end},
              {"object deletion test",
               fun() ->
                       [
                        ?assertMatch({ok, _},
                                     ?MODULE:delete(<<"b2">>, <<"k2">>, [], State)),
                        ?assertMatch({error, not_found, _},
                                     ?MODULE:get(<<"b2">>, <<"k2">>, State))
                       ]
               end
              },
              {"is_empty test",
               fun() ->
                       [
                        ?_assertEqual(false, ?MODULE:is_empty(State)),
                        ?_assertMatch({ok, _}, ?MODULE:delete(<<"b1">>,<<"k1">>, State)),
                        ?_assertMatch({ok, _}, ?MODULE:delete(<<"b3">>,<<"k3">>, State)),
                        ?_assertEqual(true, ?MODULE:is_empty(State))
                       ]
               end},
              {"bucket folding test",
               fun() ->
                       FoldBucketsFun =
                           fun(Bucket, Acc) ->
                                   [Bucket | Acc]
                           end,

                       ?_assertEqual([<<"b1">>, <<"b2">>],
                                     begin
                                         {ok, Buckets1} =
                                             ?MODULE:fold_buckets(FoldBucketsFun,
                                                                  [],
                                                                  [],
                                                                  State),
                                         lists:sort(Buckets1)
                                     end)
               end},
              {"key folding test",
               fun() ->
                       FoldKeysFun =
                           fun(Bucket, Key, Acc) ->
                                   [{Bucket, Key} | Acc]
                           end,
                       FoldKeysFun1 =
                           fun(_Bucket, Key, Acc) ->
                                   [Key | Acc]
                           end,
                       FoldKeysFun2 =
                           fun(Bucket, Key, Acc) ->
                                   case Bucket =:= <<"b1">> of
                                       true ->
                                           [Key | Acc];
                                       false ->
                                           Acc
                                   end
                           end,
                       FoldKeysFun3 =
                           fun(Bucket, Key, Acc) ->
                                   case Bucket =:= <<"b1">> of
                                       true ->
                                           Acc;
                                       false ->
                                           [Key | Acc]
                                   end
                           end,
                       [
                        ?_assertEqual([{<<"b1">>, <<"k1">>}, {<<"b2">>, <<"k2">>}],
                                      begin
                                          {ok, Keys1} =
                                              ?MODULE:fold_keys(FoldKeysFun,
                                                                [],
                                                                [],
                                                                State),
                                          lists:sort(Keys1)
                                      end),
                        ?_assertEqual({ok, [<<"k1">>]},
                                      ?MODULE:fold_keys(FoldKeysFun1,
                                                        [],
                                                        [{bucket, <<"b1">>}],
                                                        State)),
                        ?_assertEqual([<<"k2">>],
                                      ?MODULE:fold_keys(FoldKeysFun1,
                                                        [],
                                                        [{bucket, <<"b2">>}],
                                                        State)),
                        ?_assertEqual({ok, [<<"k1">>]},
                                      ?MODULE:fold_keys(FoldKeysFun2, [], [], State)),
                        ?_assertEqual({ok, [<<"k1">>]},
                                      ?MODULE:fold_keys(FoldKeysFun2,
                                                        [],
                                                        [{bucket, <<"b1">>}],
                                                        State)),
                        ?_assertEqual({ok, [<<"k2">>]},
                                      ?MODULE:fold_keys(FoldKeysFun3, [], [], State)),
                        ?_assertEqual({ok, []},
                                      ?MODULE:fold_keys(FoldKeysFun3,
                                                        [],
                                                        [{bucket, <<"b1">>}],
                                                        State))
                       ]
               end},
              {"object folding test",
               fun() ->
                       FoldKeysFun =
                           fun(Bucket, Key, Acc) ->
                                   [{Bucket, Key} | Acc]
                           end,
                       FoldObjectsFun =
                           fun(Bucket, Key, Value, Acc) ->
                                   [{{Bucket, Key}, Value} | Acc]
                           end,
                       [
                        ?_assertEqual([{<<"b1">>, <<"k1">>}],
                                      begin
                                          {ok, Keys} =
                                              ?MODULE:fold_keys(FoldKeysFun,
                                                                [],
                                                                [],
                                                                State),
                                          lists:sort(Keys)
                                      end),

                        ?_assertEqual([{{<<"b1">>,<<"k1">>}, <<"v1">>}],
                                      begin
                                          {ok, Objects1} =
                                              ?MODULE:fold_objects(FoldObjectsFun,
                                                                   [],
                                                                   [],
                                                                   State),
                                          lists:sort(Objects1)
                                      end),
                        ?_assertMatch({ok, _},
                                      ?MODULE:put(<<"b3">>, <<"k3">>, [], <<"v3">>, State)),
                        ?_assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                                       {{<<"b3">>,<<"k3">>},<<"v3">>}],
                                      begin
                                          {ok, Objects} =
                                              ?MODULE:fold_objects(FoldObjectsFun,
                                                                   [],
                                                                   [],
                                                                   State),
                                          lists:sort(Objects)
                                      end)
                       ]
               end}]
     end}.
-endif.