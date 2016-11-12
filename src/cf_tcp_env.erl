-module( cf_tcp_env ).
-behaviour( gen_fsm ).
-behaviour( cf_usr ).
-behaviour( cf_exec_env ).

-export( [start_link/1] ).
-export( [init/1, code_change/4, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [op/2] ).
-export( [submit/3] ).
-export( [halt/2] ).

-define( PROTOCOL, <<"cf_lang">> ).
-define( VSN, <<"0.1.0">> ).

%%==========================================================
%% API functions
%%==========================================================

-record( state_data, { socket, tag=undef, session=undef } ).

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

terminate( _Reason, _StateName, #state_data{ socket=Socket } ) ->
  io:format( "Closing connected socket.~n" ),
  gen_tcp:close( Socket ).


handle_event( _Event, State, StateData ) ->
  {next_state, State, StateData}.

init( Socket ) ->
  process_flag( trap_exit, true ),
  io:format( "Connecting socket.~n" ),
  {ok, preop, #state_data{ socket=Socket }}.

handle_info( {tcp, Socket, B}, preop,
             StateData=#state_data{ socket=Socket } ) ->
  io:format( "Received data: ~p~n", [B] ),
  {Tag, S} = decode( workflow, B ),
  case cf_parse:string( S ) of
    {error, Reason} ->
      ok = gen_tcp:send( Socket, encode( error, Tag, Reason ) ),
      {stop, normal, StateData};
    {ok, {Query, Rho, Gamma}} ->
      M = {?MODULE, self()},
      case cf_session:start_link( M, M, {Query, Rho, Gamma} ) of
        {error, Reason} -> error( Reason );
        {ok, Session}   ->
          {next_state, op, StateData#state_data{ session=Session, tag=Tag }}
      end
  end;

handle_info( {tcp_closed, Socket}, _, StateData=#state_data{ socket=Socket } ) ->
  {stop, normal, StateData};

handle_info( {'EXIT', _From, Reason}, _, StateData ) ->
  {stop, Reason, StateData}.


op( {halt, Result}, StateData=#state_data{ socket=Socket, tag=Tag } ) ->
  gen_tcp:send( Socket, encode( halt, Tag, Result ) ),
  {stop, normal, StateData};

op( {submit, R, App}, StateData=#state_data{ socket=Socket, tag=Tag } ) ->
  gen_tcp:send( Socket, encode( submit, Tag, {R, App} ) ),
  {next_state, op, StateData}.


%%==========================================================
%% Execution environment callback functions
%%==========================================================

submit( R, App, {?MODULE, Ref} ) ->
  gen_fsm:send_event( Ref, {submit, R, App} ).

%%==========================================================
%% User callback functions
%%==========================================================

halt( Result, {?MODULE, Ref} ) ->
  gen_fsm:send_event( Ref, {halt, Result} ).



%%==========================================================
%% Internal Functions
%%==========================================================

encode( error, Tag, {Line, Module, Reason} ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => error,
                   data     => #{ line   => Line,
                                  module => Module,
                                  reason => list_to_binary( Reason )
                                }
                 } );

encode( submit, Tag, {R, {app, AppLine, _, Lam, Fa}} ) ->

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
                   tag      => Tag,
                   msg_type => submit,
                   data     => #{
                                 id       => R,
                                 app_line => AppLine,
                                 lam_name => LamName,
                                 out_vars => OutVars,
                                 in_vars  => InVars,
                                 lang     => Lang,
                                 script   => list_to_binary( Script ),
                                 arg_map  => ArgMap
                                }
                 } );


encode( halt, Tag, {ok, ExprLst} ) ->

  L = [list_to_binary( X ) || {str, X} <- ExprLst],

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt,
                   data     => #{
                                 status => ok,
                                 result => L
                                }
                 } );

encode( halt, Tag, {error, {Line, Mod, Msg}} ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt,
                   data     => #{
                                 status => error,
                                 line   => Line,
                                 module => Mod,
                                 msg    => list_to_binary( Msg )
                                }
                 } ).

decode( workflow, B ) ->
  #{ <<"protocol">> := ?PROTOCOL,
     <<"vsn">>      := ?VSN,
     <<"tag">>      := Tag,
     <<"msg_type">> := <<"workflow">>,
     <<"data">>     := #{
                         <<"lang">> := <<"cuneiform">>,
                         <<"content">> := Wf
                       }
  } = jsone:decode( B ),

  {Tag, binary_to_list( Wf )};

decode( reply, B ) ->

  #{ <<"protocol">> := ?PROTOCOL,
     <<"vsn">>      := ?VSN,
     <<"tag">>      := Tag,
     <<"msg_type">> := <<"reply">>,
     <<"data">>     := #{
                         <<"status">> := <<"ok">>,
                         <<"id">>     := R,
                         <<"result">> := Result
                       }
  } = jsone:decode( B ),

  {Tag, R, Result}.
