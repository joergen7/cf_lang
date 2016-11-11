-module( cf_tcpport ).
-behavior( gen_fsm ).
-behavior( cf_session ).

-export( [start_link/1] ).
-export( [init/1, code_change/4, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).

-define( VSN, <<"0.1.0">> ).

%%==========================================================
%% API functions
%%==========================================================

-record( mod_info, {socket} ).

%%==========================================================
%% API functions
%%==========================================================


start_link( Socket ) ->
  gen_fsm:start_link( ?MODULE, Socket, [] ).

%%==========================================================
%% Generic FSM callback functions
%%==========================================================

code_change( _OldVsn, StateName, StateData, _Extra ) ->
  {ok, StateName, StateData}.

handle_info( _Info, StateName, StateData ) ->
  {next_state, StateName, StateData}.

handle_sync_event( _Event, _From, StateName, StateData ) ->
  {reply, {error, ignored}, StateName, StateData}.

terminate( _Reason, _StateName, _StateData ) ->
  ok.


init( Socket ) ->
  {ok, workflow, #mod_info{ socket=Socket }}.

handle_event( _Request, State, StateData ) ->
  % TODO
  {stop, nyi, StateData}.


%%==========================================================
%% Cuneiform session callback functions
%%==========================================================

submit( Ref, Msg ) ->
  gen_fsm:send_event( Ref, {submit, App} ).





%%==========================================================
%% Internal Functions
%%==========================================================


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
