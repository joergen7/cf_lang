%% -*- erlang -*-
%%
%% Cuneiform: A Functional Language for Large Scale Scientific Data Analysis
%%
%% Copyright 2016 Jörgen Brandt, Marc Bux, and Ulf Leser
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @author Jörgen Brandt <brandjoe@hu-berlin.de>

-module( cf_tcp_env ).
-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

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
  io:format( "Received workflow msg: ~p~n", [B] ),
  Workflow = decode( workflow, B ),
  #workflow{ tag=Tag, lang=cuneiform, content=Content } = Workflow,
  case cf_parse:string( binary_to_list( Content ) ) of
    {error, ErrorInfo} ->
      Halt = cf_session:error_info_to_halt_error_workflow( Tag, ErrorInfo ),
      io:format( "Sending halt_error_workflow msg: ~p~n", [encode( Halt )] ),
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
  io:format( "Received reply: ~p~n", [B] ),
  Session:reply( decode( reply, B ) ),
  {next_state, op, StateData};

handle_info( {tcp_closed, Socket}, _, StateData=#state_data{ socket=Socket } ) ->
  io:format( "TCP connection closed by other host.~n" ),
  {stop, normal, StateData};

handle_info( {'EXIT', _From, Reason}, _, StateData ) ->
  {stop, Reason, StateData}.


op( Halt=#halt_ok{}, StateData=#state_data{ socket=Socket } ) ->
  io:format( "Sending halt_ok msg: ~p~n", [Halt] ),
  gen_tcp:send( Socket, encode( Halt ) ),
  {stop, normal, StateData};

op( Halt=#halt_error_workflow{}, StateData=#state_data{ socket=Socket } ) ->
  io:format( "Sending halt_error_workflow msg: ~p~n", [Halt] ),
  gen_tcp:send( Socket, encode( Halt ) ),
  {stop, normal, StateData};

op( Halt=#halt_error_task{}, StateData=#state_data{ socket=Socket } ) ->
  io:format( "Sending halt_error_task msg: ~p~n", [Halt] ),
  gen_tcp:send( Socket, encode( Halt ) ),
  {stop, normal, StateData};

op( Submit=#submit{}, StateData=#state_data{ socket=Socket } ) ->
  io:format( "Sending submit msg: ~p~n", [Submit] ),
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

