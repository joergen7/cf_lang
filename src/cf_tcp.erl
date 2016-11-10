-module( cf_tcp ).

-export( [srv/0] ).

-define( TCP_PORT, 17489 ).
-define( LISTEN_OPT, [list, {packet, 4}, {reuseaddr, true}, {active, true}] ).
-define( VSN, <<"0.1.0">> ).

srv() ->
  case gen_tcp:listen( ?TCP_PORT, ?LISTEN_OPT ) of
    {error, Reason}    -> error( Reason );
    {ok, ListenSocket} -> listen_loop( ListenSocket )
  end.

listen_loop( ListenSocket ) ->
  case gen_tcp:accept( ListenSocket ) of
    {error, Reason} -> error( Reason );
    {ok, Socket}    ->
      Pid = spawn_link( fun() -> recv_workflow( Socket ) end ),
      case controlling_process( Socket, Pid ) of
        {error, Reason} -> error( Reason );
        ok              -> listen_loop( ListenSocket )
      end
  end.

recv_workflow( Socket ) ->
  receive
    {tcp_closed, Socket}  -> ok;
    {tcp, Socket, WorkflowStr} ->

      % Parse workflow
      case cf_parse:string( WorkflowStr ) of
        {error, Reason} ->
          gen_tcp:send( Socket, encode( {error, Reason} ) );
        {ok, {Query, Rho, Gamma}} ->
          ok
      end
  end.


encode( {error, {Line, Module, Reason}} ) ->
  jsone:encode( #{ vsn      => ?VSN,
                   msg_type => error,
                   data     => #{ line   => Line,
                                  module => Module,
                                  reason => list_to_binary( Reason )
                                }
                 } ).

