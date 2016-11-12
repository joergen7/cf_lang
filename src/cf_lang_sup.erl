-module( cf_lang_sup ).
-behaviour( supervisor ).

-export( [start_link/0, start_tcp_session/1, start_session/3] ).
-export( [init/1] ).

-define( SERVER, ?MODULE ).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link( {local, ?SERVER}, ?MODULE, [] ).

start_tcp_session( Socket ) ->
  ChildSpec = #{
                id      => {cf_tcp_env,
                            erlang:unique_integer( [positive, monotonic] )},
                start   => {cf_tcp_env, start_link, [Socket]},
                restart => temporary,
                type    => worker
               },
  supervisor:start_child( ?SERVER, ChildSpec ).

start_session( Query, Rho, Gamma ) ->
  ChildSpec = #{
                id      => {cf_session,
                            erlang:unique_integer( [positive, monotonic] )},
                start   => {cf_session, start_link, [Query, Rho, Gamma]},
                restart => temporary,
                type    => worker
               },
  supervisor:start_child( ?SERVER, ChildSpec ).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init( [] ) ->

    TcpSrv = #{
               id      => tcpsrv,
               start   => {cf_tcp_srv, start_link, []},
               restart => permanent,
               type    => worker
              },

    {ok, { {one_for_one, 10, 5}, [TcpSrv]} }.

%%====================================================================
%% Internal functions
%%====================================================================
