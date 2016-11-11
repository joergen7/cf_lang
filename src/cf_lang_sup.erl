-module( cf_lang_sup ).

-behaviour( supervisor ).

-export( [start_link/0, start_tcpsession/2] ).

%% Supervisor callbacks
-export( [init/1] ).

-define( SERVER, ?MODULE ).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link( {local, ?SERVER}, ?MODULE, [] ).

start_tcpsession( SupRef, Socket ) ->
  ChildSpec = #{
                child_id => {cf_tcp,
                             erlang:unique_integer( [positive, monotonic] )},
                start    => [cf_tcpport, start_link, [Socket]],
                restart  => temporary,
                type     => worker
               },
  supervisor:start_child( SupRef, ChildSpec ).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init( [] ) ->
    {ok, { {one_for_one, 0, 1}, []} }.

%%====================================================================
%% Internal functions
%%====================================================================
