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

-module( cf_session ).
-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

-behaviour( gen_fsm ).

-export( [start_link/3, start_link/4, reply/2, stop/1] ).
-export( [code_change/4, init/1, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [busy/2, busy/3, saturated/2, saturated/3, zombie/2, zombie/3] ).

-include( "cf_protcl.hrl" ).
-define( HASH_ALGO, sha256 ).

%%==========================================================
%% Record definitions
%%==========================================================

-record( state_data, {usr, exec_env, suppl, query, theta, seen=sets:new()} ).

%%==========================================================
%% API functions
%%==========================================================


start_link( Usr, ExecEnv,
            #workflow{ suppl=Suppl, lang=cuneiform, content=Content } ) ->

  case cf_parse:string( binary_to_list( Content ) ) of
    {error, ErrorInfo} ->
      Halt = error_info_to_halt_eworkflow( ErrorInfo ),
      {error, Halt};
    {ok, {Query, Rho, Gamma}} ->
      start_link( Usr, ExecEnv, Suppl, {Query, Rho, Gamma} )
  end.

start_link( Usr, ExecEnv, Suppl, {Query, Rho, Gamma} ) ->
  gen_fsm:start_link( ?MODULE, {Usr, ExecEnv, Suppl, {Query, Rho, Gamma}}, [] ).

reply( Reply, {?MODULE, SessionRef} ) ->
  gen_fsm:send_all_state_event( SessionRef, Reply ).

stop( {?MODULE, SessionRef} ) ->
  gen_fsm:stop( SessionRef ).


%%==========================================================
%% Generic FSM callback functions
%%==========================================================

code_change( _OldVsn, StateName, StateData, _Extra ) ->
  {ok, StateName, StateData}.

handle_info( _Info, StateName, StateData ) ->
  {next_state, StateName, StateData}.

handle_sync_event( _Event, _From, StateName, StateData ) ->
  {next_state, StateName, StateData}.


terminate( _Reason, _StateName, _StateData ) ->
  ok.

init( {Usr, ExecEnv, Suppl, {Query, Rho, Gamma}} ) ->

  % compose submit function mu
  Parent = self(),
  Mu = fun( App ) ->
         gen_fsm:sync_send_event( Parent, {submit, App} )
       end,

  % construct initial context
  Theta = {Rho, Mu, Gamma, #{}},
  _Pid = fire( Query, Theta ),

  {ok, busy, #state_data{ usr      = Usr,
                          exec_env = ExecEnv,
                          suppl    = Suppl,
                          query    = Query,
                          theta    = Theta }}.

handle_event( Reply=#reply_error{}, _StateName,
              StateData=#state_data{ usr=Usr } ) ->
  Usr:halt( reply_error_to_halt_etask( Reply ) ),
  {next_state, zombie, StateData};

handle_event( Reply=#reply_ok{}, idle,
              StateData=#state_data{ query=Query, theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  DeltaOmega = reply_ok_to_omega( Reply ),
  Omega1 = maps:merge( DeltaOmega, Omega ),
  Theta1 = {Rho, Mu, Gamma, Omega1},
  fire( Query, Theta1 ),
  {next_state, busy, StateData#state_data{ theta=Theta1 }};

handle_event( Reply=#reply_ok{}, busy,
              StateData=#state_data{ theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( reply_ok_to_omega( Reply ), Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

handle_event( Reply=#reply_ok{}, saturated,
              StateData=#state_data{ theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( reply_ok_to_omega( Reply ), Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

handle_event( _Event, zombie, StateData ) ->
  {next_state, zombie, StateData}.




% == IDLE ==
% The session is in the idle state if it is done evaluating its query and is
% waiting for new replies to come in from the execution environment.




% == BUSY ==
% The session is in the busy state when a query evaluation round has been
% started but its result has not yet been observed and if no other replies
% have arrived in the mean time.

busy( {submit, App={app, _, _, {lam, _, LamName, {sign, Lo, _}, _}, _}},
      _From, StateData=#state_data{ exec_env = ExecEnv,
                                    suppl    = Suppl,
                                    seen     = Seen } ) ->

  Submit = app_to_submit( Suppl, App ),
  #submit{ id=Hash } = Submit,

  Seen1 = case sets:is_element( Hash, Seen ) of
            true  -> Seen;
            false ->
              ExecEnv:submit( Submit ),
              sets:add_element( Hash, Seen )
          end,

  {reply, {fut, LamName, Hash, Lo}, busy, StateData#state_data{ seen=Seen1 }}.

busy( {eval, {ok, Y}}, StateData=#state_data{ usr=Usr } ) ->
  case cf_sem:pnormal( Y ) of
    true ->
      Usr:halt( expr_lst_to_halt_ok( Y ) ),
      {next_state, zombie, StateData};
    false ->
      {next_state, idle, StateData#state_data{ query=Y }}
  end;

busy( {eval, {error, ErrorInfo}}, StateData=#state_data{ usr=Usr } ) ->
  Usr:halt( error_info_to_halt_eworkflow( ErrorInfo ) ),
  {next_state, zombie, StateData}.


% == SATURATED ==
% The session is in the saturated state if a query evaluation round has been
% started and one or more replies have already arrived even though the
% evaluation round is still going on.

saturated( {submit, App={app, _, _, {lam, _, LamName, {sign, Lo, _}, _}, _}},
           _From, StateData=#state_data{ exec_env = ExecEnv,
                                         suppl    = Suppl,
                                         seen     = Seen } ) ->

  Submit = app_to_submit( Suppl, App ),
  #submit{ id=Hash } = Submit,

  Seen1 = case sets:is_element( Hash, Seen ) of
            true  -> Seen;
            false ->
              ExecEnv:submit( Hash, Submit ),
              sets:add_element( Hash, Seen )
          end,

  {reply, {fut, LamName, Hash, Lo}, saturated,
          StateData#state_data{ seen=Seen1 }}.

saturated( {eval, {ok, Y}}, StateData=#state_data{ theta=Theta } ) ->
  fire( Y, Theta ),
  {next_state, busy, StateData#state_data{ query=Y } };

saturated( {eval, {error, ErrorInfo}},
           StateData=#state_data{ usr=Usr } ) ->
  Usr:halt( error_info_to_halt_eworkflow( ErrorInfo ) ),
  {next_state, zombie, StateData}.


% == ZOMBIE ==
% The session is in the zombie state if either (i) the workflow terminated and
% the result has been communicated to the user, (ii) workflow evaluation has
% resulted in an error, or (iii) any of the received replies was an error reply.

zombie( _Event, _From, StateData ) ->
  {next_state, zombie, StateData}.

zombie( _Event, StateData ) ->
  {next_state, zombie, StateData}.


%%==========================================================
%% Internal Functions
%%==========================================================

-spec fire( Query::[cf_sem:expr()], Theta::cf_sem:ctx() ) -> pid().

fire( Query, Theta ) ->

  Parent = self(),
  F = fun() ->
        gen_fsm:send_event( Parent, {eval, cf_sem:eval( Query, Theta )} )
      end,

  spawn_link( F ).


-spec hash( App::cf_sem:app() ) -> binary().

hash( {app, _, _, {lam, _, _, {sign, Lo, Li}, Body}, Fa} ) ->
  B = term_to_binary( {Lo, Li, Body, Fa} ),
  <<X:256/big-unsigned-integer>> = crypto:hash( ?HASH_ALGO, B ),
  list_to_binary( io_lib:format( "~.16B", [X] ) ).


-spec app_to_submit( Suppl::_, App::cf_sem:app() ) -> #submit{}.

app_to_submit( Suppl, App={app, AppLine, _, Lam, Fa} ) ->

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

  R = hash( App ),

  #submit{ suppl    = Suppl,
           id       = R,
           app_line = AppLine,
           lam_name = list_to_binary( LamName ),
           out_vars = OutVars,
           in_vars  = InVars,
           lang     = Lang,
           script   = list_to_binary( Script ),
           arg_map  = ArgMap }.


-spec expr_lst_to_halt_ok( ExprLst ) -> #halt_ok{}
when ExprLst :: [cf_sem:expr()].

expr_lst_to_halt_ok( ExprLst ) ->
  #halt_ok{ result=[list_to_binary( S ) ||{str, S} <- ExprLst] }.


-spec reply_error_to_halt_etask( Reply ) -> #halt_etask{}
when Reply :: #reply_error{}.

reply_error_to_halt_etask( #reply_error{ id       = Id,
                                         app_line = AppLine,
                                         lam_name = LamName,
                                         output   = Output,
                                         script   = Script } ) ->

  #halt_etask{ id       = Id,
               app_line = AppLine,
               lam_name = LamName,
               output   = Output,
               script   = Script }.


-spec reply_ok_to_omega( Reply::#reply_ok{} ) -> #{string() => [cf_sem:expr()]}.

reply_ok_to_omega( #reply_ok{ id=R, result_map=ResultMap } ) ->

  F = fun( OutName, ValueLst, Acc ) ->
        Acc#{ {binary_to_list( OutName ), R} =>
              [{str, binary_to_list( V )} || V <- ValueLst] }
      end,

  maps:fold( F, #{}, ResultMap ).


-spec error_info_to_halt_eworkflow( ErrorInfo ) ->
  #halt_eworkflow{}
when ErrorInfo :: {pos_integer(), atom(), string()}.

error_info_to_halt_eworkflow( {Line, Module, Reason} ) ->
  #halt_eworkflow{ line   = Line,
                   module = Module,
                   reason = list_to_binary( Reason ) }.
