-module( cf_tcp_env ).
-behavior( gen_fsm ).
-behavior( cf_usr ).
-behavior( cf_exec_env ).

-export( [start_link/1] ).
-export( [init/1, code_change/4, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [submit/2] ).

-define( PROTOCOL, <<"cf_lang">> ).
-define( VSN, <<"0.1.0">> ).

%%==========================================================
%% API functions
%%==========================================================

-record( state_data, { socket, session=undef } ).

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


handle_sync_event( _Event, _From, StateName, StateData ) ->
  {reply, {error, ignored}, StateName, StateData}.

terminate( _Reason, _StateName, _StateData ) ->
  ok.


handle_event( {submit, App}, op, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, encode( submit, App ) ),
  {next_state, op, StateData};

handle_event( _Event, State, StateData ) ->
  {next_state, State, StateData}.

init( Socket ) ->
  {ok, preop, #state_data{ socket=Socket }}.

handle_info( {tcp, Socket, S}, preop,
             StateData=#state_data{ socket=Socket } ) ->
  case cf_parse:string( S ) of
    {error, Reason} ->
      gen_tcp:send( Socket, encode( error, Reason ) );
    {ok, {Query, Rho, Gamma}} ->
      M = {?MODULE, self()},
      case cf_session:start_link( M, M, {Query, Rho, Gamma} ) of
        {error, Reason} -> error( Reason );
        {ok, Session}   ->
          {next_state, op, StateData#state_data{ session=Session }}
      end
  end;

handle_info( {tcp_closed, Socket}, _, StateData=#state_data{ socket=Socket } ) ->
  {stop, normal, StateData};

handle_info( _Info, State, StateData ) ->
  {next_state, State, StateData}.


%%==========================================================
%% Execution environment callback functions
%%==========================================================

submit( App, {?MODULE, Ref} ) ->
  gen_fsm:send_event( Ref, {submit, App} ).

halt( Result, {?MODULE, Ref} ) ->
  gen_fsm:send_event( Ref, {halt, Result} ).



%%==========================================================
%% Internal Functions
%%==========================================================

encode( error, {Line, Module, Reason} ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => error,
                   data     => #{ line   => Line,
                                  module => Module,
                                  reason => list_to_binary( Reason )
                                }
                 } );

encode( submit, {app, AppLine, _, Lam, Fa} ) ->

  {lam, _, LamName, Sign, Body} = Lam,
  {sign, Lo, Li} = Sign,
  {forbody, Lang, Script} = Body,

  OutVars = [#{ name=>list_to_binary( N ), is_file=>Pf, is_list=>Pl }
             || {param, {name, N, Pf}, Pl} <- Lo],

  InVars = [#{ name=>list_to_binary( N ), is_file=>Pf, is_list=>Pl }
            || {param, {name, N, Pf}, Pl} <- Li],

  F = fun( N, V, Acc ) ->
        Acc#{ list_to_binary( N ) => [list_to_binary( S ) ||{str, S} <- V] }
      end,

  ArgMap = maps:fold( F, #{}, Fa ),

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => submit,
                   data     => #{
                                 app_line => AppLine,
                                 lam_name => LamName,
                                 out_vars => OutVars,
                                 in_vars  => InVars,
                                 lang     => Lang,
                                 script   => list_to_binary( Script ),
                                 arg_map  => ArgMap
                                }
                 } ).
