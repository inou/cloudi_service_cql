-module(cassandra_SUITE).
-author('Maciek Wcislik <maciekwcislik@gmail.com>').

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("erlcql/include/erlcql.hrl").


-compile([export_all]).

-define(CHECKSPEC(M,F,A), true = proper:check_spec({M,F,A})).
-define(PROPTEST(A), true = proper:quickcheck(A())).
-define(PROPTEST(A, B), true = proper:quickcheck(A(B))).

-define(NUMTEST, 1000).
-define(TIMEOUT, 1000).

-define(DB_PREFIX, "/db/cassandra/").
-define(DB_TARGET, "testdb").
-define(DB_KEYSPACE, <<"test_ks">>).

-define(CREATE_KEYSPACE, [<<"CREATE KEYSPACE IF NOT EXISTS ">>, ?DB_KEYSPACE,
                        <<" WITH replication = {'class': 'SimpleStrategy', ",
                                                        "'replication_factor': 1}">>]).
-define(DROP_KEYSPACE, [<<"DROP KEYSPACE IF EXISTS ">>, ?DB_KEYSPACE]).
-define(CREATE_TABLE, [<<"CREATE TABLE IF NOT EXISTS ">>, ?DB_KEYSPACE, $., ?DB_TARGET,
                                                <<"(k int PRIMARY KEY, v text)">>]).
-define(DROP_TABLE, [<<"DROP TABLE IF EXISTS ">>, ?DB_KEYSPACE, $., ?DB_TARGET]).

-define(CS_CONFIG(Prefix, Target),
        {internal, Prefix, cloudi_service_cql, [{cass_keyspace, ?DB_KEYSPACE},
                                                {cass_tablename, Target}], 
                immediate_closest, 5000, 5000, 5000, undefined, undefined, 1, 5, 300, []}).



suite() ->
    [{timetrap, {seconds, 40}}].

init_per_suite(Config) ->
    setup_cloudi(Config),
    setup_cloudi_services(Config),
    Context = cloudi:new(),
    Service = ?DB_PREFIX ++ ?DB_TARGET,
    {ok, _} = cloudi_service_cql:query(Context, Service, ?DROP_KEYSPACE),
    {ok, _} = cloudi_service_cql:query(Context, Service, ?CREATE_KEYSPACE),
    {ok, _} = cloudi_service_cql:query(Context, Service, ?CREATE_TABLE),
    Config.


end_per_suite(Config) ->
    teardown_cloudi(Config),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    [{context, cloudi:new()} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{cassandra_group, [], [
                            t_insert_doc,
                            t_service_spec
                           ]}].

all() ->
    [
        {group, cassandra_group}
    ].

%% proper tests
t_service_spec(_Config) ->
    [] = proper:check_specs(cloudi_service_cql).

t_insert_doc(Config) ->
    ?PROPTEST(prop_insert_doc, Config).

prop_insert_doc(Config) ->
    ?FORALL(Doc, document(),
            begin
                {<<"k">>, Key} = lists:keyfind(<<"k">>, 1, Doc),
                {<<"v">>, Value} = lists:keyfind(<<"v">>, 1, Doc),
                Context = ?config(context, Config),
                Service = ?DB_PREFIX ++ ?DB_TARGET,
                {ok, _Response} = cloudi_service_cql:insert_doc(Context, Service, Doc),
                {ok, {reply, Response, _State}} = cloudi_service_cql:query(Context, Service, [<<"SELECT * FROM ">>,
                                                                             ?DB_KEYSPACE, $., ?DB_TARGET,
                                                                             <<" WHERE k=">>, integer_to_binary(Key)], one),
                {ok, {Rows, _ColNames}} = Response,
                1 = length(Rows),
                [Key,Value] = hd(Rows),
                true
            end).

%% generators
key() ->
    {<<"k">>, pos_integer()}.
chr() ->
    range(48,126).
proper_string() ->
    ?LET(S, list(chr()), list_to_binary(S)).

value() ->
    {<<"v">>, proper_string()}.
document() ->
    [key(), value()].


%% setup & teardown
setup_cloudi_services(Config) ->
    cloudi_service_api:services_add([?CS_CONFIG(?DB_PREFIX, ?DB_TARGET)], ?TIMEOUT),
    timer:sleep(?TIMEOUT),
    [{db_prefix, ?DB_PREFIX}, {target, ?DB_TARGET} | Config].

setup_cloudi(_Config) ->
    ok = reltool_util:application_start(cloudi_core).

teardown_cloudi(_Config) ->
    reltool_util:application_stop(cloudi_core).



