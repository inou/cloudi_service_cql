%%%-------------------------------------------------------------------
%%% @author Maciek Wcislik <maciekwcislik@gmail.com>
%%% @copyright (C) 2013 Ubiquiti Networks, Inc.
%%% @doc Module for inserting callhomes into cassandra
%%% @end
%%%-------------------------------------------------------------------
-module(cloudi_service_cql).
-behaviour(cloudi_service).

-compile([{parse_transform, lager_transform}]).

-define(CASS_KEYSPACE, <<"test_ks">>).
-define(CASS_TABLENAME, <<"test_table">>).
-define(CASS_POOL_NAME, ?MODULE).
-define(CASS_POOL_OPTIONS, {[{size, 10},{max_overflow, 20}],
                            [{host, "127.0.0.1"},{port, 9042}]}).

-export([insert_doc/3]).
-export([query/3, query/4]).

%% cloudi_service callbacks
-export([cloudi_service_init/3,
         cloudi_service_handle_request/11,
         cloudi_service_handle_info/3,
         cloudi_service_terminate/2]).

-record(state, {keyspace   :: binary(),
                table_name :: binary(),
                pool_name  :: atom()
               }).

-type state()                       :: #state{}.

-type insert_value()                :: binary()
                                    | iolist()
                                    | integer()
                                    | float().

-type insert_document()             :: [{binary(), insert_value()}].
-type query()                       :: iolist().
-type body()                        :: insert_document() 
                                    | query().
-type request()                     :: {atom(), body()}
                                    | {atom(), body(), atom()}.
-type consistency()                 :: atom().

-type dispatcher()                  :: cloudi_service:dispatcher() 
                                    | cloudi:context().
-type name()                        :: cloudi_service:service_name().
%%%===================================================================
%%% API
%%%===================================================================

-spec insert_doc(dispatcher(), name(), insert_document()) ->
    {ok, {reply, erlcql:response(), state()}}
    | {error, term()}.
insert_doc(Dispatcher, Name, Doc) ->
    cloudi:send_sync(Dispatcher, Name, {insert_doc, Doc}).


-spec query(dispatcher(), name(), query()) ->
    {ok, {reply, erlcql:response(), state()}}
    | {error, term()}.
query(Dispatcher, Name, Query) ->
    cloudi:send_sync(Dispatcher, Name, {query, Query}).

-spec query(dispatcher(), name(), query(), consistency()) ->
    {ok, {reply, erlcql:response(), state()}}
    | {error, term()}.
query(Dispatcher, Name, Query, Consistency) ->
    cloudi:send_sync(Dispatcher, Name, {query, Query, Consistency}).

%%%===================================================================
%%% cloudi_service callbacks
%%%===================================================================

cloudi_service_init(Args, _Prefix, Dispatcher) ->
    Defaults = [{cass_keyspace, ?CASS_KEYSPACE},
                {cass_tablename, ?CASS_TABLENAME},
                {pool_name, ?CASS_POOL_NAME},
                {pool_options, ?CASS_POOL_OPTIONS}],
    [KS, TN, PN, {SArgs, WArgs}] = cloudi_proplists:take_values(Defaults, Args),
    {ok, _} = erlcql_poolboy:start_link(PN, SArgs, WArgs),
    cloudi_service:subscribe(Dispatcher, "*"),
    {ok, #state{keyspace=KS, table_name=TN, pool_name=PN}}.

cloudi_service_handle_request(_Type, _Name, _Pattern, _RequestInfo, Request,
                              _Timeout, _Priority, _TransId, _Pid,
                              State, _Dispatcher) ->
    {reply, handle_request(Request, State), State}.

cloudi_service_handle_info(_Request, State, _) ->
    {noreply, State}.

cloudi_service_terminate(_, State) ->
    ok = poolboy:stop(State#state.pool_name),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_request(request(), state()) ->
    {reply, any(), state()}.
handle_request({insert_doc, Doc}, State) ->
    Doc1 = lists:ukeysort(1, Doc),
    {Columns, Values} = prepare_query(Doc1),
    Query = create_insert_query(Columns, Values, State),
    Reply = erlcql_poolboy:q(State#state.pool_name, Query),
    {reply, Reply, State};

handle_request({query, Query}, State) ->
    Reply = erlcql_poolboy:q(State#state.pool_name, Query),
    {reply, Reply, State};

handle_request({query, Query, Consistency}, State) ->
    Reply = erlcql_poolboy:q(State#state.pool_name, Query, Consistency),
    {reply, Reply, State}.

-spec interweave(list(), any()) -> list().
interweave([], _Elem) ->
    [];
interweave([H|T], Elem) ->
    interweave(T, Elem, [H]).

interweave([H|T], Elem, Acc) ->
    interweave(T, Elem, [H,Elem|Acc]);

interweave([], _Elem, Acc) ->
    lists:reverse(Acc).

-spec prepare_query([{binary(),any()}]) -> {[binary()],[binary()]}.
prepare_query(Doc) ->
    lists:foldl(fun({Col, Val}, {Columns, Values}) when is_binary(Val) ->
                        {[Col|Columns], [[$', Val, $'] | Values]};
                   ({Col, Val}, {Columns, Values}) when is_integer(Val) ->
                        {[Col|Columns], [integer_to_binary(Val)|Values]};
                   ({Col, Val}, {Columns, Values}) when is_float(Val) ->
                        {[Col|Columns], [float_to_binary(Val)|Values]};
                   ({Col, Val}, {Columns, Values}) when is_list(Val) ->
                        {[Col|Columns], [Val|Values]};
                   (_, CV) -> CV
                end, {[],[]}, Doc).

-spec create_insert_query(iolist(), iolist(), any()) -> iolist().
create_insert_query(Cols, Vals, #state{keyspace=K, table_name=TN}) ->
    C = interweave(Cols, $,),
    V = interweave(Vals, $,),
    Q0 = [<<"INSERT INTO ">>, K,<<".">>, TN, <<"(">>],
    Q1 = [<<") VALUES (">>],
    Q2 = [<<")">>],
    [Q0,C, Q1, V, Q2].
