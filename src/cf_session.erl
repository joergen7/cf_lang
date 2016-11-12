-module( cf_session ).
-behaviour( gen_fsm ).

-export( [start_link/3, reply/2] ).
-export( [code_change/4, init/1, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).
-export( [idle/2, busy/2, saturated/2] ).

-define( HASH_ALGO, sha256 ).

%%==========================================================
%% Record definitions
%%==========================================================

-record( state_data, {usr, exec_env, query, theta, seen=sets:new()} ).

%%==========================================================
%% API functions
%%==========================================================

start_link( Usr, ExecEnv, {Query, Rho, Gamma} ) ->
  gen_fsm:start_link( ?MODULE, {Usr, ExecEnv, {Query, Rho, Gamma}}, [] ).

reply( Session, Reply ) ->
  gen_fsm:send_event( Session, {reply, Reply} ).


%%==========================================================
%% Generic FSM callback functions
%%==========================================================

code_change( _OldVsn, StateName, StateData, _Extra ) ->
  {ok, StateName, StateData}.

handle_info( _Info, StateName, StateData ) ->
  {next_state, StateName, StateData}.

handle_sync_event( {submit,
                    App={app, _, _, {lam, _, LamName, {sign, Lo, _}, _}, _}},
                   _From, StateName,
                   StateData=#state_data{ exec_env=ExecEnv, seen=Seen } ) ->

  Hash = hash( App ),

  Seen1 = case sets:is_element( Hash, Seen ) of
            true  -> Seen;
            false ->
              ExecEnv:submit( Hash, App ),
              sets:add_element( Hash, Seen )
          end,

  {reply, {fut, LamName, Hash, Lo}, StateName,
          StateData#state_data{ seen=Seen1 }}.


terminate( _Reason, _StateName, _StateData ) ->
  ok.

handle_event( _Event, State, StateData ) ->
  {next_state, State, StateData}.


init( {Usr, ExecEnv, {Query, Rho, Gamma}} ) ->

  % compose submit function mu
  Parent = self(),
  Mu = fun( App ) ->
         {ok, Fut} = gen_fsm:sync_send_all_state_event( Parent, {submit, App} ),
         Fut
       end,

  % construct initial context
  Theta = {Rho, Mu, Gamma, #{}},
  _Pid = fire( Query, Theta ),

  {ok, busy, #state_data{ usr      = Usr,
                          exec_env = ExecEnv,
                          query    = Query,
                          theta    = Theta }}.



idle( {reply, {ok, ReplyMap}},
      StateData=#state_data{ query=Query, theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( ReplyMap, Omega )},
  fire( Query, Theta1 ),
  {next_state, busy, StateData#state_data{ theta=Theta1 }}.




busy( {reply, {ok, ReplyMap}}, StateData=#state_data{ theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( ReplyMap, Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

busy( {eval, {ok, Y}}, StateData=#state_data{ usr=Usr } ) ->
  case cf_sem:pnormal( Y ) of
    true ->
      Usr:halt( {ok, Y} ),
      {stop, normal, StateData};
    false ->
      {next_state, idle, StateData#state_data{ query=Y }}
  end;

busy( {eval, {error, Reason}}, StateData=#state_data{ usr=Usr } ) ->
  Usr:halt( {error, Reason} ),
  {stop, normal, StateData}.





saturated( {reply, {ok, ReplyMap}},
           StateData=#state_data{ theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( ReplyMap, Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

saturated( {eval, {ok, Y}}, StateData=#state_data{ theta=Theta } ) ->
  fire( Y, Theta ),
  {next_state, busy, StateData#state_data{ query=Y } };

saturated( {eval, {error, Reason}}, StateData=#state_data{ usr=Usr } ) ->
  Usr:halt( {error, Reason} ),
  {stop, normal, StateData}.



%%==========================================================
%% Internal Functions
%%==========================================================

fire( Query, Theta ) ->

  Parent = self(),
  F = fun() ->
        gen_fsm:send_event( Parent, {eval, cf_sem:eval( Query, Theta )} )
      end,

  spawn_link( F ).

hash( {app, _, _, {lam, _, _, {sign, Lo, Li}, B}, Fa} ) ->
  B = term_to_binary( {Lo, Li, B, Fa} ),
  <<X:256/big-unsigned-integer>> = crypto:hash( ?HASH_ALGO, B ),
  list_to_binary( io_lib:format( "~.16B", [X] ) ).




