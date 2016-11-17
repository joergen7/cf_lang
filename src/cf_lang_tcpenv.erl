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

-module( cf_lang_tcpenv ).
-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

-behaviour( gen_fsm ).
-behaviour( cf_usr ).
-behaviour( cf_execenv ).

-export( [start_link/1, stop/1] ).
-export( [init/1, code_change/4, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [op/2] ).
-export( [submit/2] ).
-export( [halt/2] ).

-include( "cf_protcl.hrl" ).

%%==========================================================
%% Record definitions
%%==========================================================

-record( state_data, { socket, session } ).

%%==========================================================
%% API functions
%%==========================================================


start_link( Socket ) ->
  gen_fsm:start_link( ?MODULE, Socket, [] ).

stop( {?MODULE, Ref} ) ->
  gen_fsm:stop( Ref ).

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
  Workflow = cf_protcl:decode( workflow, B ),
  M = {?MODULE, self()},
  case cf_lang_session:start_link( M, M, Workflow ) of
    {error, Halt=#halt_eworkflow{}} ->
      ok = gen_tcp:send( Socket, cf_protcl:encode( Halt ) );
    {error, Reason} ->
      error( Reason );
    {ok, SessionRef} ->
      {next_state, op,
                   StateData#state_data{ session={cf_lang_session, SessionRef} }}
  end;

handle_info( {tcp, Socket, B}, op,
             StateData=#state_data{ socket=Socket, session=Session } ) ->
  Session:reply( cf_protcl:decode( reply, B ) ),
  {next_state, op, StateData};

handle_info( {tcp_closed, Socket}, _, StateData=#state_data{ socket=Socket } ) ->
  {stop, normal, StateData};

handle_info( {'EXIT', _From, Reason}, _, StateData ) ->
  {stop, Reason, StateData}.


op( Halt=#halt_ok{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, cf_protcl:encode( Halt ) ),
  {stop, normal, StateData};

op( Halt=#halt_eworkflow{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, cf_protcl:encode( Halt ) ),
  {stop, normal, StateData};

op( Halt=#halt_etask{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, cf_protcl:encode( Halt ) ),
  {stop, normal, StateData};

op( Submit=#submit{}, StateData=#state_data{ socket=Socket } ) ->
  gen_tcp:send( Socket, cf_protcl:encode( Submit ) ),
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


