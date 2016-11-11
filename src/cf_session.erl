-module( cf_session ).
-behavior( gen_fsm ).

-export( [start_link/3, reply/2] ).
-export( [code_change/4, init/1, handle_event/3, handle_info/3,
          handle_sync_event/4, terminate/3] ).

%%==========================================================
%% Record definitions
%%==========================================================

-record( state_data, {query, theta} ).


%% =========================================================
%% Callback function declarations
%% =========================================================

-callback submit( Ref::pid(), Msg::term() ) -> ok.


%%==========================================================
%% API functions
%%==========================================================

start_link( Query, Rho, Gamma ) ->
  gen_fsm:start_link( ?MODULE, {Query, Rho, Gamma}, [] ).

reply( Session, Reply ) ->
  gen_fsm:send_event( Session, {reply, Reply} ).


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


init( {Query, Rho, Gamma} ) ->

  % compose submit function mu
  Parent = self(),
  Mu = fun( App ) ->
         gen_fsm:send_event( Parent, {submit, App} )
       end,

  % construct initial context
  Theta = {Rho, Mu, Gamma, #{}},

  {ok, busy, #state_data{ query=Query, theta=Theta }}.


handle_event( {eval, {ok, Y}}, busy, StateData ) ->
  {next_state, idle, StateData#state_data{ query=Y }};

handle_event( {eval, {ok, Y}}, saturated,
              StateData=#state_data{ theta=Theta } ) ->
  fire( Y, Theta ),
  {next_state, busy, StateData#state_data{ query=Y } };

handle_event( {eval, {error, Reason}}, _, StateData ) ->
  % TODO
  {stop, nyi, StateData};

handle_event( {reply, {ok, ReplyMap}}, idle,
              StateData=#state_data{ query=Query, theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( ReplyMap, Omega )},
  fire( Query, Theta1 ),
  {next_state, busy, StateData#state_data{ theta=Theta1 }};

handle_event( {reply, {ok, ReplyMap}}, busy,
              StateData=#state_data{ theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( ReplyMap, Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

handle_event( {reply, {ok, ReplyMap}}, saturated,
              StateData=#state_data{ theta=Theta } ) ->
  {Rho, Mu, Gamma, Omega} = Theta,
  Theta1 = {Rho, Mu, Gamma, maps:merge( ReplyMap, Omega )},
  {next_state, saturated, StateData#state_data{ theta=Theta1 }};

handle_event( {reply, {error, Reason}}, _, StateData ) ->
  % TODO
  {stop, nyi, StateData};

handle_event( {submit, App}, _, StateData=#state_data{} ) ->
  % TODO
  {stop, nyi, StateData}.



%%==========================================================
%% Internal Functions
%%==========================================================

fire( Query, Theta ) ->

  Parent = self(),
  F = fun() ->
        gen_fsm:send_event( Parent, {eval, cf_sem:eval( Query, Theta )} )
      end,

  spawn_link( F ).