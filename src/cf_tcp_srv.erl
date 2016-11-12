-module( cf_tcp_srv ).

-define( TCP_PORT, 17489 ).
-define( LISTEN_OPT, [binary, {packet, 4}, {reuseaddr, true}, {active, true}] ).

-export( [start_link/0] ).

%%==========================================================
%% API functions
%%==========================================================

start_link() ->
  io:format( "Starting server.~n" ),
  case gen_tcp:listen( ?TCP_PORT, ?LISTEN_OPT ) of
    {error, Reason}    -> io:format( "~p~n", [Reason] ), error( Reason );
    {ok, ListenSocket} ->
      {ok, spawn_link( fun() -> listen_loop( ListenSocket ) end )}
  end.

%%==========================================================
%% Internal Functions
%%==========================================================

listen_loop( ListenSocket ) ->
  process_flag( trap_exit, true ),
  case gen_tcp:accept( ListenSocket, 500 ) of
    {error, timeout} ->
      receive
        _ ->
          io:format( "Closing listen socket.~n" ),
          gen_tcp:close( ListenSocket )
      after 0 ->
        listen_loop( ListenSocket )
      end;
    {error, Reason}  -> error( Reason );
    {ok, Socket}     ->
      {ok, Child} = cf_lang_sup:start_tcp_session( Socket ),
      case gen_tcp:controlling_process( Socket, Child ) of
        {error, Reason} -> error( Reason );
        ok              -> listen_loop( ListenSocket )
      end
  end.

