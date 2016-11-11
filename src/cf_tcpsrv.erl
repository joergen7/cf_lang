-module( cf_tcpsrv ).

-define( TCP_PORT, 17489 ).
-define( LISTEN_OPT, [list, {packet, 4}, {reuseaddr, true}, {active, true}] ).

-export( [start_link/1] ).

%%==========================================================
%% API functions
%%==========================================================

start_link( SupRef ) ->
  case gen_tcp:listen( ?TCP_PORT, ?LISTEN_OPT ) of
    {error, Reason}    -> error( Reason );
    {ok, ListenSocket} ->
      start_link( fun() -> listen_loop( ListenSocket, SupRef ) end )
  end.

%%==========================================================
%% Internal Functions
%%==========================================================

listen_loop( ListenSocket, SupRef ) ->
  case gen_tcp:accept( ListenSocket ) of
    {error, Reason} -> error( Reason );
    stop            -> ok;
    {ok, Socket}    ->
      {ok, Child} = cf_lang_sup:start_tcpsession( SupRef, Socket ),
      case gen_tcp:controlling_process( Socket, Child ) of
        {error, Reason} -> error( Reason );
        ok              -> listen_loop( ListenSocket, SupRef )
      end
  end.

