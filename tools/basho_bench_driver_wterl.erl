-module(basho_bench_driver_wterl).

-record(state, { connection, uri }).

-export([new/1,
         run/4]).

-include_lib("basho_bench/include/basho_bench.hrl").


%% ====================================================================
%% API
%% ====================================================================

new(1) ->
    %% Make sure wterl is available
    case code:which(wterl) of
        non_existing ->
            ?FAIL_MSG("~s requires wterl to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,
    {ok, _} = wterl_sup:start_link(),
    setup(1);
new(Id) ->
    setup(Id).

setup(_Id) ->
    %% Get the target directory
    Dir = basho_bench_config:get(wterl_dir, "/tmp"),
    Config = basho_bench_config:get(wterl, []),
    Uri = config_value(table_uri, Config, "lsm:test"),
    ConnectionOpts = config_value(connection, Config, [{create, true}]),
    SessionOpts = config_value(session, Config, []),
    TableOpts = config_value(table, Config, []),

    %% Start WiredTiger
    Connection =
        case wterl_conn:is_open() of
            false ->
                case wterl_conn:open(Dir, ConnectionOpts, SessionOpts) of
                    {ok, Conn} ->
                        Conn;
                    {error, Reason0} ->
                        ?FAIL_MSG("Failed to establish a WiredTiger connection, wterl backend unable to start: ~p\n", [Reason0])
                end;
            true ->
                {ok, Conn} = wterl_conn:get(),
                Conn
        end,
    case wterl:create(Connection, Uri, TableOpts) of
        ok ->
            {ok, #state{connection=Connection, uri=Uri}};
        {error, Reason} ->
            {error, Reason}
    end.

run(get, KeyGen, _ValueGen, #state{connection=Connection, uri=Uri}=State) ->
    case wterl:get(Connection, Uri, KeyGen()) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, #state{connection=Connection, uri=Uri}=State) ->
    case wterl:put(Connection, Uri, KeyGen(), ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(delete, KeyGen, _ValueGen, #state{connection=Connection, uri=Uri}=State) ->
    case wterl:delete(Connection, Uri, KeyGen()) of
        ok ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

config_value(Key, Config, Default) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            Default;
        Value ->
            Value
    end.
