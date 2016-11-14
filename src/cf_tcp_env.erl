-module( cf_tcp_env ).
-behaviour( gen_fsm ).
-behaviour( cf_usr ).
-behaviour( cf_exec_env ).

-export( [start_link/1] ).
-export( [init/1, code_change/4, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [op/2] ).
-export( [submit/2] ).
-export( [halt/2] ).

-include( "cf_lang.hrl" ).

-define( PROTOCOL, <<"cf_lang">> ).
-define( VSN, <<"0.1.0">> ).

%%==========================================================
%% API functions
%%==========================================================

-record( state_data, { socket, session } ).

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
  Workflow = decode( workflow, B ),
  #workflow{ tag=Tag, lang=cuneiform, content=Content } = Workflow,
  case cf_parse:string( binary_to_list( Content ) ) of
    {error, ErrorInfo} ->
      Halt = cf_session:error_info_to_halt_error_workflow( Tag, ErrorInfo ),
      ok = gen_tcp:send( Socket, encode( Halt ) ),
      {stop, normal, StateData};
    {ok, {Query, Rho, Gamma}} ->
      M = {?MODULE, self()},
      case cf_session:start_link( M, M, Tag, {Query, Rho, Gamma} ) of
        {error, Reason}  -> error( Reason );
        {ok, SessionRef} ->
          {next_state, op,
                       StateData#state_data{ session={cf_session, SessionRef} }}
      end
  end;

handle_info( {tcp, Socket, B}, op,
             StateData=#state_data{ socket=Socket, session=Session } ) ->
  Session:reply( decode( reply, B ) ),
  {next_state, op, StateData};

handle_info( {tcp_closed, Socket}, _, StateData=#state_data{ socket=Socket } ) ->
  {stop, normal, StateData};

handle_info( {'EXIT', _From, Reason}, _, StateData ) ->
  {stop, Reason, StateData}.


op( Halt=#halt_ok{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, encode( Halt ) ),
  {stop, normal, StateData};

op( Halt=#halt_error_workflow{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, encode( Halt ) ),
  {stop, normal, StateData};

op( Halt=#halt_error_task{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, encode( Halt ) ),
  {stop, normal, StateData};

op( Submit=#submit{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, encode( Submit ) ),
  {next_state, op, StateData}.


%%==========================================================
%% Execution environment callback functions
%%==========================================================

submit( Submit, {?MODULE, Ref} ) ->
  gen_fsm:send_event( Ref, Submit ).

%%==========================================================
%% User callback functions
%%==========================================================

halt( Halt, {?MODULE, Ref} ) ->
  gen_fsm:send_event( Ref, Halt ).

%%==========================================================
%% Internal Functions
%%==========================================================

encode( #halt_ok{ tag    = Tag,
                  result = Result } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt_ok,
                   data     => #{ result => Result }
                 } );

encode( #halt_error_workflow{ tag    = Tag,
                              line   = Line,
                              module = Module,
                              reason = Reason} ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt_error_workflow,
                   data     => #{ line   => Line,
                                  module => Module,
                                  reason => Reason
                                }
                 } );

encode( #halt_error_task{ tag      = Tag,
                          id       = R,
                          app_line = AppLine,
                          lam_name = LamName,
                          script   = Script,
                          output   = Output } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt_error_task,
                   data     => #{
                                 id       => R,
                                 app_line => AppLine,
                                 lam_name => LamName,
                                 script   => Script,
                                 output   => Output
                                }
                 } );


encode( #submit{ tag      = Tag,
                 id       = R,
                 app_line = AppLine,
                 lam_name = LamName,
                 out_vars = OutVars,
                 in_vars  = InVars,
                 lang     = Lang,
                 script   = Script,
                 arg_map  = ArgMap } ) ->


  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => submit,
                   data     => #{ id       => R,
                                  app_line => AppLine,
                                  lam_name => LamName,
                                  out_vars => OutVars,
                                  in_vars  => InVars,
                                  lang     => Lang,
                                  script   => Script,
                                  arg_map  => ArgMap
                                }
                 } ).





decode( workflow, B ) ->
  #{ <<"protocol">> := ?PROTOCOL,
     <<"vsn">>      := ?VSN,
     <<"tag">>      := Tag,
     <<"msg_type">> := <<"workflow">>,
     <<"data">>     := #{ <<"lang">>    := <<"cuneiform">>,
                          <<"content">> := Content
                        }
  } = jsone:decode( B ),

  #workflow{ tag=Tag, lang=cuneiform, content=Content };

decode( reply, B ) ->

  case jsone:decode( B ) of

    #{ <<"protocol">> := ?PROTOCOL,
       <<"vsn">>      := ?VSN,
       <<"tag">>      := Tag,
       <<"msg_type">> := <<"reply_ok">>,
       <<"data">>     := #{ <<"id">>         := R,
                            <<"result_map">> := ResultMap
                          }
     } -> #reply_ok{ tag=Tag, id=R, result_map=ResultMap };

    #{ <<"protocol">> := ?PROTOCOL,
       <<"vsn">>      := ?VSN,
       <<"tag">>      := Tag,
       <<"msg_type">> := <<"reply_error">>,
       <<"data">>     := #{ <<"id">>         := R,
                            <<"output">>     := Output,
                            <<"app_line">>   := AppLine,
                            <<"lam_name">> := LamName,
                            <<"script">>     := Script
                          }
    } -> #reply_error{ tag      = Tag,
                       id       = R,
                       app_line = AppLine,
                       lam_name = LamName,
                       script   = Script,
                       output   = Output }
  end.

