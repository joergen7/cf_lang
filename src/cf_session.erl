-module( cf_session ).
-behaviour( gen_fsm ).

-export( [start_link/4, reply/2, stop/1, error_info_to_halt_error_workflow/2] ).
-export( [code_change/4, init/1, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [busy/2, busy/3, saturated/2, saturated/3, zombie/2, zombie/3] ).

-include( "cf_lang.hrl" ).
-define( HASH_ALGO, sha256 ).

%%==========================================================
%% Record definitions
%%==========================================================

-record( state_data, {usr, exec_env, tag, query, theta, seen=sets:new()} ).

%%==========================================================
%% API functions
%%==========================================================

start_link( Usr, ExecEnv, Tag, {Query, Rho, Gamma} ) ->
  gen_fsm:start_link( ?MODULE, {Usr, ExecEnv, Tag, {Query, Rho, Gamma}}, [] ).

reply( Reply, {?MODULE, SessionRef} ) ->
  gen_fsm:send_all_state_event( SessionRef, Reply ).

stop( {?MODULE, SessionRef} ) ->
  gen_fsm:stop( SessionRef ).


-spec error_info_to_halt_error_workflow( Tag, ErrorInfo ) ->
  #halt_error_workflow{}
when Tag       :: binary(),
     ErrorInfo :: {pos_integer(), atom(), string()}.

error_info_to_halt_error_workflow( Tag, {Line, Module, Reason} ) ->
  #halt_error_workflow{ tag    = Tag,
                        line   = Line,
                        module = Module,
                        reason = list_to_binary( Reason ) }.

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

handle_event( Reply=#reply_error{ tag=Tag }, _StateName,
              StateData=#state_data{ usr=Usr, tag=Tag } ) ->
  Usr:halt( reply_error_to_halt_error_task( Tag, Reply ) ),
  {next_state, zombie, StateData};

handle_event( Reply=#reply_ok{ tag=Tag }, idle,
              StateData=#state_data{ query=Query, theta=Theta, tag=Tag } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( reply_ok_to_omega( Reply ), Omega )},
  fire( Query, Theta1 ),
  {next_state, busy, StateData#state_data{ theta=Theta1 }};

handle_event( Reply=#reply_ok{ tag=Tag }, busy,
              StateData=#state_data{ theta=Theta, tag=Tag } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( reply_ok_to_omega( Reply ), Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

handle_event( Reply=#reply_ok{ tag=Tag }, saturated,
              StateData=#state_data{ theta=Theta, tag=Tag } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( reply_ok_to_omega( Reply ), Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

handle_event( _Event, zombie, StateData ) ->
  {next_state, zombie, StateData}.


init( {Usr, ExecEnv, Tag, {Query, Rho, Gamma}} ) ->

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
                          tag      = Tag,
                          query    = Query,
                          theta    = Theta }}.



% == IDLE ==
% The session is in the idle state if it is done evaluating its query and is
% waiting for new replies to come in from the execution environment.




% == BUSY ==
% The session is in the busy state when a query evaluation round has been
% started but its result has not yet been observed and if no other replies
% have arrived in the mean time.

busy( {submit, App={app, _, _, {lam, _, LamName, {sign, Lo, _}, _}, _}},
      _From, StateData=#state_data{ exec_env = ExecEnv,
                                    seen     = Seen,
                                    tag      = Tag } ) ->

  Submit = app_to_submit( Tag, App ),
  #submit{ id=Hash } = Submit,

  Seen1 = case sets:is_element( Hash, Seen ) of
            true  -> Seen;
            false ->
              ExecEnv:submit( Submit ),
              sets:add_element( Hash, Seen )
          end,

  {reply, {fut, LamName, Hash, Lo}, busy, StateData#state_data{ seen=Seen1 }}.

busy( {eval, {ok, Y}}, StateData=#state_data{ usr=Usr, tag=Tag } ) ->
  case cf_sem:pnormal( Y ) of
    true ->
      Usr:halt( expr_lst_to_halt_ok( Tag, Y ) ),
      {next_state, zombie, StateData};
    false ->
      {next_state, idle, StateData#state_data{ query=Y }}
  end;

busy( {eval, {error, ErrorInfo}}, StateData=#state_data{ usr=Usr, tag=Tag } ) ->
  Usr:halt( error_info_to_halt_error_workflow( Tag, ErrorInfo ) ),
  {next_state, zombie, StateData}.


% == SATURATED ==
% The session is in the saturated state if a query evaluation round has been
% started and one or more replies have already arrived even though the
% evaluation round is still going on.

saturated( {submit, App={app, _, _, {lam, _, LamName, {sign, Lo, _}, _}, _}},
           _From, StateData=#state_data{ exec_env = ExecEnv,
                                         seen     = Seen,
                                         tag      = Tag } ) ->

  Submit = app_to_submit( Tag, App ),
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
           StateData=#state_data{ usr=Usr, tag=Tag } ) ->
  Usr:halt( error_info_to_halt_error_workflow( Tag, ErrorInfo ) ),
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


-spec app_to_submit( Tag::binary(), App::cf_sem:app() ) -> #submit{}.

app_to_submit( Tag, App={app, AppLine, _, Lam, Fa} ) ->

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

  #submit{ tag      = Tag,
           id       = R,
           app_line = AppLine,
           lam_name = list_to_binary( LamName ),
           out_vars = OutVars,
           in_vars  = InVars,
           lang     = Lang,
           script   = list_to_binary( Script ),
           arg_map  = ArgMap }.


-spec expr_lst_to_halt_ok( Tag, ExprLst ) -> #halt_ok{}
when Tag     :: binary(),
     ExprLst :: [cf_sem:expr()].

expr_lst_to_halt_ok( Tag, ExprLst ) ->
  #halt_ok{ tag=Tag, result=[list_to_binary( S ) ||{str, S} <- ExprLst] }.


-spec reply_error_to_halt_error_task( Tag, Reply ) -> #halt_error_task{}
when Tag   :: binary(),
     Reply :: #reply_error{}.

reply_error_to_halt_error_task( Tag,
                                #reply_error{ tag      = Tag,
                                              id       = Id,
                                              app_line = AppLine,
                                              lam_name = LamName,
                                              output   = Output,
                                              script   = Script } ) ->

  #halt_error_task{ tag      = Tag,
                    id       = Id,
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