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

%% @author Jorgen Brandt <brandjoe@hu-berlin.de>


-module( cf_sem ).
-author( "Jorgen Brandt <brandjoe@hu-berlin.de>" ).


-export( [eval/2, pnormal/1] ).

-ifdef( TEST ).
-include_lib( "eunit/include/eunit.hrl" ).
-endif.

%% =============================================================================
%% Abstract Syntax
%% =============================================================================

%% Expression %% ===============================================================

-type expr()    :: str() | var() | select() | cnd() | app().                    % (1)
-type str()     :: {str, S::string()}.                                          % (2)
-type var()     :: {var, Line::pos_integer(), N::string()}.                     % (3)
-type param()   :: {param, M::name(), Pl::boolean()}.                           % (4)
-type fut()     :: {fut, LamName::string(), R::binary(), Lo::[param()]}.        % (5)
-type select()  :: {select, AppLine::pos_integer(), C::pos_integer(), U::fut()}.% (6)
-type cnd()     :: {cnd, Line::pos_integer(),                                   % (7)
                         Xc::[expr()], Xt::[expr()], Xe::[expr()]}.
-type app()     :: {app, AppLine::pos_integer(), C::pos_integer(),              % (8)
                         Lambda::lam() | var(), Fa::#{string() => [expr()]}}.

%% Task Signature %% ===========================================================

-type correl()  :: {correl, Lc::[name()]}.                                      % (9)
-type inparam() :: param() | correl().                                          % (10)
-type sign()    :: {sign, Lo::[param()], Li::[inparam()]}.                      % (11)

%% Lambda Term %% ==============================================================

-type lam()     :: {lam, LamLine::pos_integer(), LamName::string(),             % (12)
                         S::sign(), B::body()}.


-type name()    :: {name, N::string(), Pf::boolean()}.

% Task Body
-type body()    :: natbody() | forbody().                                       % (13)
-type natbody() :: {natbody, Fb::#{string() => [expr()]}}.                      % (14)
-type forbody() :: {forbody, L::lang(), S::string()}.                           % (15)
-type lang()    :: bash | python | r.                                           % (16)

%% Evaluation Context %% =======================================================

-type ctx()     :: {Rho   :: #{string() => [expr()]},                           % (17)
                    Mu    :: fun( ( app() ) -> fut() ),
                    Gamma :: #{string() => lam()},
                    Omega :: #{{string(), binary()} => [expr()]}}.

%% =============================================================================
%% Predicates
%% =============================================================================

%% Normal Form %% ==============================================================

-spec pnormal( X ) -> boolean()                                                 % (18)
when X :: #{string() => [expr()]} | [expr()] | expr().

pnormal( F ) when is_map( F )  -> pnormal( maps:values( F ) );                  % (19)
pnormal( L ) when is_list( L ) -> lists:all( fun pnormal/1, L );                % (20,21)
pnormal( {str, _S} )           -> true;                                         % (22)
pnormal( _T )                  -> false.

%% Singularity %% ==============================================================

-spec psing( A::app() ) -> boolean().                                           % (61)

psing( {app, _, _, {lam, _, _, {sign, _, []}, _}, _} ) -> true;                 % (62)
psing( {app, AppLine, C,                                                        % (63)
             {lam, LamLine, LamName, {sign, Lo, [{param, _, Pl}|T]}, B},
             Fa} )
when Pl ->
  psing( {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, T}, B}, Fa} );
psing( {app, AppLine, C,                                                        % (64)
             {lam, LamLine, LamName, {sign, Lo, [{param, {name, N, _}, _}|T]},
                   B},
             Fa} ) ->
  case maps:is_key( N, Fa ) of
    false -> throw( {AppLine, ?MODULE,
                     "argument "++N++" is unbound in application of "
                     ++LamName} );
    true  ->
      case length( maps:get( N, Fa ) ) of
        1 -> psing( {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, T}, B},
                          Fa} );
        _ -> false
      end
  end;
psing( _ ) -> false.

%% Enumerability %% ============================================================

-spec pen( X::expr()|[expr()] ) -> boolean().                                   % (65)

pen( X )when is_list( X ) -> lists:all( fun pen/1, X );                         % (66,67)
pen( {str, _S} )          -> true;                                              % (68)
pen( {cnd, _, _Xc, Xt, Xe} ) when length( Xt ) =:= 1, length( Xe ) =:= 1 ->     % (69)
  pen( Xt ) andalso pen( Xe );
pen( X={app, _, C, {lam, _, _, {sign, Lo, _Li}, _B}, _Fb} ) ->                  % (70)
  case psing( X ) of
    false -> false;
    true ->
      {param, _, Pl} = lists:nth( C, Lo ),
      not Pl
  end;
pen( {select, _, C, {fut, _, _R, Lo}} ) ->                                      % (71)
  {param, _, Pl} = lists:nth( C, Lo ),
  not Pl;
pen( _T ) -> false.

%% Context Independence %% =====================================================

-spec pindep( X ) -> boolean()                                                  % (81)
when X :: #{string() => [expr()]} | [expr()] | expr().

pindep( Fa ) when is_map( Fa ) -> pindep( maps:values( Fa ) );                  % (82)
pindep( X ) when is_list( X )  -> lists:all( fun pindep/1, X );                 % (83,84)
pindep( {str, _} )             -> true;                                         % (85)
pindep( {select, _, _, _} )    -> true;                                         % (86)
pindep( {cnd, _, Xc, Xt, Xe} ) ->                                               % (87)
  pindep( Xc ) andalso pindep( Xt ) andalso pindep( Xe );
pindep( {app, _, _, _, Fa} )   -> pindep( Fa );                                 % (88)
pindep( _ )                    -> false.

%% =============================================================================
%% Evaluation
%% =============================================================================

%% The eval Function %% ========================================================

-spec eval( X, Theta ) -> {ok, [expr()]} | {error, {pos_integer(), string()}}
when X     :: [expr()],
     Theta :: ctx().

eval( X, Theta ) ->
  try step( X, Theta ) of
    X -> {ok, X};
    X1 -> eval( X1, Theta )
  catch
    throw:{Line, ?MODULE, Reason} -> {error, {Line, ?MODULE, Reason}}
  end.

%% Reduction Rules %% ==========================================================



-spec step( X, Theta ) -> #{string() => [expr()]} | [expr()]                    % (42)
when X     :: #{string() => [expr()]} | [expr()] | expr(),
     Theta :: ctx().

% Argument map
step( Fa, Theta ) when is_map( Fa ) ->                                          % (23,24)
  maps:map( fun( _N, X ) -> step( X, Theta ) end, Fa );

% Expression List
step( X, Theta ) when is_list( X ) ->                                           % (25,26)
  lists:flatmap( fun( Y ) -> step( Y, Theta ) end, X );

% String Literal
step( X={str, _S}, _Theta ) -> [X];                                             % (27)

% Variable
step( {var, Line, N}, {Rho, _Mu, _Gamma, _Omega} ) ->                           % (28)
  case maps:is_key( N, Rho ) of
    true  -> maps:get( N, Rho );
    false -> throw( {Line, ?MODULE, "unbound variable "++N} )
  end;

% Future Channel Selection
step( S={select, _, C, {fut, _, R, Lo}}, {_Rho, _Mu, _Gamma, Omega} ) ->        % (29,30)
  {param, {name, N, _}, _} = lists:nth( C, Lo ),
  maps:get( {N, R}, Omega, [S] );

% Conditional
step( {cnd, _, [], _Xt, Xe}, _Theta ) -> Xe;                                    % (31)
step( {cnd, Line, Xc=[_|_], Xt, Xe}, Theta ) ->
  case pnormal( Xc ) of
    false -> [{cnd, Line, step( Xc, Theta ), Xt, Xe}];                          % (32)
    true  -> Xt                                                                 % (33)
  end;

% Application (early enumeration and tail recursion)
step( {app, Line, C, {var, _, N}, Fa}, {_Rho, _Mu, Gamma, _Omega} ) ->          % (52)
  case maps:is_key( N, Gamma ) of
    true  -> [{app, Line, C, maps:get( N, Gamma ), Fa}];
    false -> throw( {Line, ?MODULE, "undefined task "++N} )
  end;

step( X={app, AppLine, C,
              Lambda={lam, LamLine, LamName, S={sign, Lo, _Li}, B},
              Fa},
      Theta={_Rho, Mu, Gamma, Omega} ) ->
  case psing( X ) of
    false -> enum( [{app, AppLine, C, Lambda, step( Fa, Theta )}] );            % (89)
    true  ->
      case B of
        {forbody, _L, _Z} ->
          case pnormal( Fa ) of
            false -> [{app, AppLine, C, Lambda, step( Fa, Theta )}];            % (90)
            true  -> [{select, AppLine, C, apply( Mu, [X] )}]                   % (91)
          end;
        {natbody, Fb} ->
          case pindep( Fa ) of
            false -> [{app, AppLine, C, Lambda, step( Fa, Theta )}];            % (92)
            true  ->
              {param, {name, N, _}, Pl} = lists:nth( C, Lo ),
              case maps:is_key( N, Fb ) of
                false -> throw( {LamLine, ?MODULE,
                                 "undefined output parameter "++N++" in task "
                                 ++LamName} );
                true  ->
                  #{N := V0} = Fb,
                  V1 = step( V0, {maps:merge( Fb, Fa ), Mu, Gamma, Omega} ),
                  case pindep( V1 ) of
                    false -> [{app, AppLine, C,                                 % (93)
                                    {lam, LamLine, LamName, S,
                                          {natbody, Fb#{ N => V1 }}},
                                    Fa}];
                    true  ->
                      case Pl orelse length( V1 ) =:= 1 of
                        true  -> V1;                                            % (94)
                        false -> throw( {LamLine, ?MODULE,
                                         "signature mismatch when evaluating task "
                                         ++LamName
                                         ++": singleton output value expected for"
                                         ++N} )
                      end
                  end
              end
          end
      end
  end.



%% =============================================================================
%% Enumeration
%% =============================================================================

%% The enum Function %%

-spec enum( A::[app()] ) -> [app()].                                            % (40)

enum( Z ) ->
  Z1 = estep( Z ),
  case Z1 of
    Z -> Z;                                                                     % (41)
    _ -> enum( Z1 )                                                             % (42)
  end.

%% Enumeration Rules (early enumeration) %%

-spec estep( A ) -> [app()]                                                     % (43)
when A :: app() | [app()].

estep( A ) when is_list( A ) ->                                                 % (44,45)
  lists:flatmap( fun( B ) -> estep( B ) end, A );
estep( X={app, _, _, {lam, _, _, {sign, _, []}, _}, _} ) -> [X];                % (46)
  
estep( {app, AppLine, C,                                                        % (47)
             {lam, LamLine, LamName, {sign, Lo, [H={param, _, Pl}|T]}, B}, Fa} )
when Pl ->
  aug_lst( estep( {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, T}, B},
                        Fa} ), H );
estep( X={app, AppLine, C,
               {lam, LamLine, LamName,
                     {sign, Lo, Li=[H={param, {name, N, _}, _Pl}|T]},
                     B},
               Fa} ) ->
  #{ N := V } = Fa,
  case pen( V ) of
    true  ->
      case length( V ) of
        1 -> aug_lst( estep( {app, AppLine, C,                                  % (72)
                                   {lam, LamLine, LamName,
                                         {sign, Lo, T},
                                         B},
                                   Fa} ), H );
        _ -> [{app, AppLine, C,                                                 % (73)
                    {lam, LamLine, LamName,
                          {sign, Lo, Li},
                          B},
                    Fa#{ N => [Y] }} || Y <- V]
      end;
    false -> [X]                                                                % (74)
  end;
estep( X={app, AppLine, C,
               {lam, LamLine, LamName,
                     {sign, Lo, [H={correl, Lc}|T]}, B}, Fa} )
when length( Lc ) > 1 ->
  Pen = pen( [maps:get( N, Fa ) || {name, N, _} <- Lc] ),
  case Pen of
    false -> [X];                                                               % (75)
    true  ->
      Z = corr( Lc, Fa ),
      aug_lst( [{app, AppLine, C,                                               % (76)
                      {lam, LamLine, LamName,
                            {sign, Lo, T},
                            B},
                      G} || G <- Z], H )
  end.



%% Augmentation %%

-spec aug_lst( Z::[app()], A::inparam() ) -> [app()].                           % (50)

aug_lst( Z, A ) -> [aug( X, A ) || X <- Z].                                     % (51)

-spec aug( X::app(), A::inparam() ) -> app().                                   % (52)

aug( {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, Li}, B}, Fa},         % (53)
     A={param, _, _Pl} ) ->
  {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, [A|Li]}, B}, Fa};

aug( {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, Li}, B}, Fa},         % (54)
     {correl, Lc} ) ->
  L1 = [{param, N, false} || N <- Lc],
  {app, AppLine, C, {lam, LamLine, LamName, {sign, Lo, L1++Li}, B}, Fa}.

%% Correlation %%

-spec corr( Lc, F ) -> [#{string() => [expr()]}]
when Lc :: [name()],
     F  :: #{string() => [expr()]}.

corr( Lc=[{name, N, _}|_], F ) ->
  case maps:get( N, F ) of
    [] -> [];
    _  ->
      {Fstar, Fminus} = corrstep( Lc, F, F ),
      [Fstar|corr( Lc, Fminus )]
  end.

-spec corrstep( Lc, Fstar, Fminus ) ->                                          % (58)
  {#{string() => [expr()]}, #{string() => [expr()]}}
when Lc     :: [name()],
     Fstar  :: #{string() => [expr()]},
     Fminus :: #{string() => [expr()]}.

corrstep( [], Fstar, Fminus ) -> {Fstar, Fminus};                               % (59)
corrstep( [{name, N, _}|T], Fstar, Fminus ) ->                                  % (60)
  #{ N := [A|B] } = Fminus,
  corrstep( T, Fstar#{ N => [A] }, Fminus#{ N => B } ).




%% =============================================================================
%% Unit Tests
%% =============================================================================

-ifdef( TEST ).


%% =============================================================================
%% Predicates
%% =============================================================================

%% Finality %%

str_should_be_normal_test() ->
  S = {str, "blub"},
  ?assert( pnormal( S ) ).

app_should_not_be_normal_test() ->
  A = {app, 12, 1, {var, "f"}, #{}},
  ?assertNot( pnormal( A ) ).

cnd_should_not_be_normal_test() ->
  C = {cnd, 12, [{str, "a"}], [{str, "b"}], [{str, "c"}]},
  ?assertNot( pnormal( C ) ).

select_should_not_be_normal_test() ->
  Fut = {fut, "f", 1234, [{param, "out", false}]},
  S = {select, 12, 1, Fut},
  ?assertNot( pnormal( S ) ).

var_should_not_be_normal_test() ->
  V = {var, 12, "x"},
  ?assertNot( pnormal( V ) ).

all_str_should_be_normal_test() ->
  X = [{str, "bla"}, {str, "blub"}],
  ?assert( pnormal( X ) ).

empty_lst_should_be_normal_test() ->
  ?assert( pnormal( [] ) ).

one_var_lst_should_not_be_normal_test() ->
  X = [{str, "bla"}, {str, "blub"}, {var, 12, "x"}],
  ?assertNot( pnormal( X ) ).

all_var_lst_should_not_be_normal_test() ->
  X = [{var, 10, "bla"}, {var, 11, "blub"}, {var, 12, "x"}],
  ?assertNot( pnormal( X ) ).

empty_map_should_be_normal_test() ->
  ?assert( pnormal( #{} ) ).

only_str_map_should_be_normal_test() ->
  M = #{"x" => [{str, "bla"}, {str, "blub"}], "y" => [{str, "shalala"}]},
  ?assert( pnormal( M ) ).

one_var_map_should_not_be_normal_test() ->
  M = #{"x" => [{str, "bla"}, {str, "blub"}],
        "y" => [{str, "shalala"}, {var, 12, "x"}]},
  ?assertNot( pnormal( M ) ).

all_var_map_should_not_be_normal_test() ->
  M = #{"x" => [{var, 10, "bla"}, {var, 11, "blub"}],
        "y" => [{var, 12, "shalala"}, {var, 13, "x"}]},
  ?assertNot( pnormal( M ) ).

%% Singularity %%

app_without_arg_should_be_singular_test() ->
  S = {sign, [{param, {name, "out", false}, false}], []},
  B = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", S, B},
  App = {app, 13, 1, Lam, #{}},
  ?assert( psing( App ) ).

app_binding_single_str_should_be_singular_test() ->
  S = {sign, [{param, {name, "out", false}, false}], [{param, {name, "x", false}, false}]},
  B = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", S, B},
  App = {app, 13, 1, Lam, #{"x" => [{str, "bla"}]}},
  ?assert( psing( App ) ).

app_binding_str_lst_should_not_be_singular_test() ->
  S = {sign, [{param, {name, "out", false}, false}], [{param, {name, "x", false}, false}]},
  B = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", S, B},
  App = {app, 13, 1, Lam, #{"x" => [{str, "bla"}, {str, "blub"}]}},
  ?assertNot( psing( App ) ).

app_with_only_aggregate_args_should_be_singular_test() ->
  S = {sign, [{param, {name, "out", false}, false}], [{param, {name, "x", false}, true}]},
  B = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", S, B},
  App = {app, 13, 1, Lam, #{"x" => [{str, "bla"}, {str, "blub"}]}},
  ?assert( psing( App ) ).

%% Enumerability %%

empty_lst_should_be_enumerable_test() ->
  ?assert( pen( [] ) ).

str_lst_should_be_enumerable_test() ->
  ?assert( pen( [{str, "a"}, {str, "b"}] ) ).

one_var_lst_should_not_be_enumerable_test() ->
  ?assertNot( pen( [{str, "a"}, {var, 12, "x"}] ) ).

all_var_lst_should_not_be_enumerable_test() ->
  ?assertNot( pen( [{var, 12, "x"}, {var, 13, "y"}] ) ).

str_should_be_enumerable_test() ->
  ?assert( pen( {str, "blub"} ) ).

single_str_branch_cnd_should_be_enumerable_test() ->
  ?assert( pen( {cnd, 12, [], [{str, "a"}], [{str, "b"}]} ) ).

empty_then_branch_cnd_should_not_be_enumerable_test() ->
  ?assertNot( pen( {cnd, 12, [], [], [{str, "b"}]} ) ).

empty_else_branch_cnd_should_not_be_enumerable_test() ->
  ?assertNot( pen( {cnd, 12, [], [{str, "a"}]} ) ).

select_non_lst_app_should_be_enumerable_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", Sign, Body},
  App = {app, 13, 1, Lam, #{}},
  ?assert( pen( App ) ).

select_lst_app_should_not_be_enumerable_test() ->
  Sign = {sign, [{param, {name, "out", false}, true}], []},
  Body = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", Sign, Body},
  App = {app, 13, 1, Lam, #{}},
  ?assertNot( pen( App ) ).

non_singular_app_should_not_be_enumerable_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], [{param, {name, "x", false}, false}]},
  Body = {forbody, bash, "shalala"},
  Lam = {lam, 12, "f", Sign, Body},
  Fa = #{"x" => [{str, "bla"}, {str, "blub"}]},
  App = {app, 13, 1, Lam, Fa},
  ?assertNot( pen( App ) ).

non_lst_select_should_be_enumerable_test() ->
  Lo = [{param, {name, "out", false}, false}],
  Fut = {fut, "f", 1234, Lo},
  Select = {select, 12, 1, Fut},
  ?assert( pen( Select ) ).

lst_select_should_not_be_enumerable_test() ->
  Lo = [{param, {name, "out", false}, true}],
  Fut = {fut, "f", 1234, Lo},
  Select = {select, 12, 1, Fut},
  ?assertNot( pen( Select ) ).

%% =============================================================================
%% Enumeration
%% =============================================================================

%% The enum Function %%

enum_without_app_does_nothing_test() ->
  S = {sign, [{param, {param, "out", false}, false}], []},
  B = {forbody, bash, "shalala"},
  Lam = {lam, 1, "f", S, B},
  App = {app, 2, 1, Lam, #{}},
  ?assertEqual( [App], enum( App ) ).

enum_empty_input_param_creates_single_instance_test() ->
  Li = [],
  Lo = [{param, {name, "out", false}, false}],
  Fa = #{},
  Sign = {sign, Lo, Li},
  Body = {forbody, bash, "blub"},
  Lam = {lam, 20, "f", Sign, Body},
  App = {app, 10, 1, Lam, Fa},

  ?assertEqual( [App], enum( [App] ) ).

enum_single_input_param_single_value_creates_single_instance_test() ->
  Li = [{param, {name, "a", false}, false}],
  Lo = [{param, {name, "out", false}, false}],
  Fa = #{"a" => [{str, "A"}]},
  Sign = {sign, Lo, Li},
  Body = {forbody, bash, "blub"},
  Lam = {lam, 20, "f", Sign, Body},
  App = {app, 10, 1, Lam, Fa},

  ?assertEqual( [App], enum( [App] ) ).

%% Enumeration Rules %%

%% Augmentation %%

can_aug_with_param_test() ->
  Lo = [{param, {name, "out", false}, false}],
  B = {forbody, bash, "blub"},
  L0 = [{param, {name, "b", false}, false}, {param, {name, "c", false}, false}],
  F = #{"a" => [{str, "1"}], "b" => [{str, "2"}], "c" => [{str, "3"}]},
  App = {app, 10, 1, {lam, 20, "f", {sign, Lo, L0}, B}, F},
  I = {param, {name, "a", false}, false},
  L1 = [I|L0],
  ?assertEqual( {app, 10, 1, {lam, 20, "f", {sign, Lo, L1}, B}, F}, aug( App, I ) ).

can_aug_with_correl_test() ->
  Lo = [{param, {name, "out", false}, false}],
  B = {forbody, bash, "blub"},
  L0 = [{param, {name, "b", false}, false}, {param, {name, "c", false}, false}],
  F = #{"a1" => [{str, "11"}],
        "a2" => [{str, "12"}],
        "b" => [{str, "2"}],
        "c" => [{str, "3"}]},
  App = {app, 10, 1, {lam, 20, "f", {sign, Lo, L0}, B}, F},
  I = {correl, [{name, "a1", false}, {name, "a2", false}]},
  L1 = [{param, {name, "a1", false}, false}, {param, {name, "a2", false}, false}|L0],
  ?assertEqual( {app, 10, 1, {lam, 20, "f", {sign, Lo, L1}, B}, F}, aug( App, I ) ).

can_augment_empty_inparamlst_with_param_test() ->
  Lo = [{param, {name, "out", false}, false}],
  B = {forbody, bash, "blub"},
  F1 = #{"a" => [{str, "x1"}]},
  F2 = #{"a" => [{str, "y1"}]},
  AppList = [{app, 10, 1, {lam, 20, "f", {sign, Lo, []}, B}, F1},
             {app, 30, 1, {lam, 40, "g", {sign, Lo, []}, B}, F2}],
  I = {param, {name, "a", false}, false},
  L1 = [{param, {name, "a", false}, false}],
  ?assertEqual( [{app, 10, 1, {lam, 20, "f", {sign, Lo, L1}, B}, F1},
                 {app, 30, 1, {lam, 40, "g", {sign, Lo, L1}, B}, F2}],
                aug_lst( AppList, I ) ).

can_augment_inparamlst_with_param_test() ->
  Lo = [{param, {name, "out", false}, false}],
  B = {forbody, bash, "blub"},
  L0 = [{param, {name, "b", false}, false}, {param, {name, "c", false}, false}],
  F1 = #{"a" => [{str, "x1"}], "b" => [{str, "x2"}], "c" => [{str, "x3"}]},
  F2 = #{"a" => [{str, "y1"}], "b" => [{str, "y2"}], "c" => [{str, "y3"}]},
  AppList = [{app, 10, 1, {lam, 20, "f", {sign, Lo, L0}, B}, F1},
             {app, 30, 1, {lam, 40, "g", {sign, Lo, L0}, B}, F2}],
  I = {param, {name, "a", false}, false},
  L1 = [{param, {name, "a", false}, false}|L0],
  X = [{app, 10, 1, {lam, 20, "f", {sign, Lo, L1}, B}, F1},
       {app, 30, 1, {lam, 40, "g", {sign, Lo, L1}, B}, F2}],
  ?assertEqual( X, aug_lst( AppList, I ) ).

can_augment_inparamlst_with_correl_test() ->
  Lo = [{param, {name, "out", false}, false}],
  B = {forbody, bash, "blub"},
  L0 = [{param, {name, "b", false}, false}, {param, {name, "c", false}, false}],
  F1 = #{"a1" => [{str, "x11"}],
         "a2" => [{str, "x12"}],
         "b" => [{str, "x2"}],
         "c" => [{str, "x3"}]},
  F2 = #{"a1" => [{str, "y11"}],
         "a2" => [{str, "y12"}],
         "b" => [{str, "y2"}],
         "c" => [{str, "y3"}]},
  AppList = [{app, 10, 1, {lam, 20, "f", {sign, Lo, L0}, B}, F1},
             {app, 30, 1, {lam, 40, "g", {sign, Lo, L0}, B}, F2}],
  I = {correl, [{name, "a1", false}, {name, "a2", false}]},
  L1 = [{param, {name, "a1", false}, false}, {param, {name, "a2", false}, false}|L0],
  X = [{app, 10, 1, {lam, 20, "f", {sign, Lo, L1}, B}, F1},
       {app, 30, 1, {lam, 40, "g", {sign, Lo, L1}, B}, F2}],
  ?assertEqual( X, aug_lst( AppList, I ) ).

%% Correlation %%

corrstep_should_separate_first_value_single_test() ->
  Lc = [{name, "a", false}],
  F0 = #{"a" => [{str, "1"}, {str, "2"}, {str, "3"}]},
  Y = {#{"a" => [{str, "1"}]}, #{"a" => [{str, "2"}, {str, "3"}]}},
  X = corrstep( Lc, F0, F0 ),
  ?assertEqual( Y, X ).

corrstep_should_separate_first_value_two_test() ->
  Lc = [{name, "a", false}, {name, "b", false}],
  F0 = #{"a" => [{str, "1"}, {str, "2"}, {str, "3"}], "b" => [{str, "A"}, {str, "B"}, {str, "C"}]},
  Y = {#{"a" => [{str, "1"}], "b" => [{str, "A"}]}, #{"a" => [{str, "2"}, {str, "3"}], "b" => [{str, "B"}, {str, "C"}]}},
  X = corrstep( Lc, F0, F0 ),
  ?assertEqual( Y, X ).








-define( THETA0, {#{}, fun mu/1, #{}, #{}} ).

mu( {app, _AppLine, _C, {lam, _LamLine, LamName, {sign, Lo, Li}, _B}, _Fa} ) ->

  IsParam = fun( {param, _N, _Pl} ) -> true;
               ( {correl, _Lc} )    -> false
            end,

  case lists:all( IsParam, Li ) of
    false -> error( invalid_correl );
    true  ->  {fut, LamName, rand:uniform( 1000000000 ), Lo}
  end.




nil_should_eval_itself_test() ->
  ?assertEqual( {ok, []}, eval( [], ?THETA0 ) ).

str_should_eval_itself_test() ->
  E = [{str, "bla"}],
  ?assertEqual( {ok, E}, eval( E, ?THETA0 ) ).

undef_var_should_fail_test() ->
  E = [{var, 1, "x"}],
  ?assertMatch( {error, {1, ?MODULE, _}}, eval( E, ?THETA0 ) ).

def_var_should_eval_to_bound_value_test() ->
  E = [{str, "blub"}],
  X = eval( [{var, 2, "x"}], {#{"x" => E}, fun mu/1, #{}, #{}} ),
  ?assertEqual( {ok, E}, X ).

def_var_should_cascade_binding_test() ->
  E = [{str, "blub"}],
  Theta = {#{"x" => [{var, 2, "y"}], "y" => E}, fun mu/1, #{}, #{}},
  X = eval( [{var, 3, "x"}], Theta ),
  ?assertEqual( {ok, E}, X ).

def_var_should_cascade_binding_twice_test() ->
  A = [{str, "A"}],
  Rho = #{"x" => [{var, 2, "y"}], "y" => [{var, 3, "z"}], "z" => A},
  ?assertEqual( {ok, A}, eval( [{var, 4, "x"}], {Rho, fun mu/1, #{}, #{}} ) ).

unfinished_fut_should_eval_to_itself_test() ->
  Fut = {fut, "f", 1234, [{param, {name, "out", false}, false}]},
  E = [{select, 2, 1, Fut}],
  X = eval( E, ?THETA0 ),
  ?assertEqual( {ok, E}, X ).


finished_fut_should_eval_to_result_test() ->
  Fut = {fut, "f", 1234, [{param, {name, "out", false}, false}]},
  S = {select, 2, 1, Fut},
  F = [{str, "blub"}],
  Theta = {#{}, fun mu/1, #{}, #{{"out", 1234} => F}},
  X = eval( [S], Theta ),
  ?assertEqual( {ok, F}, X ).

noarg_fn_should_eval_plain_test() ->
  E = [{str, "bla"}],
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {natbody, #{"out" => E}},
  Lam = {lam, 2, "f", Sign, Body},
  F = [{app, 3, 1, Lam, #{}}],
  ?assertEqual( {ok, E}, eval( F, ?THETA0 ) ).

noarg_fn_should_eval_body_test() ->
  E = [{str, "bla"}],
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {natbody, #{"out" => [{var, 2, "x"}], "x" => E}},
  Lam = {lam, 3, "f", Sign, Body},
  F = [{app, 4, 1, Lam, #{}}],
  ?assertEqual( {ok, E}, eval( F, ?THETA0 ) ).

fn_call_should_insert_lam_test() ->
  E = [{str, "bla"}],
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {natbody, #{"out" => E}},
  Lam = {lam, 2, "f", Sign, Body},
  F = [{app, 3, 1, {var, 4, "f"}, #{}}],
  Theta = {#{}, fun mu/1, #{"f" => Lam}, #{}},
  ?assertEqual( {ok, E}, eval( F, Theta ) ).

app_with_unbound_lam_should_fail_test() ->
  F = [{app, 1, 1, {var, 2, "f"}, #{}}],
  ?assertMatch( {error, {1, ?MODULE, _}}, eval( F, ?THETA0 ) ).

identity_fn_should_eval_arg_test() ->
  E = [{str, "bla"}],
  Sign = {sign, [{param, {name, "out", false}, false}], [{param, {name, "inp", false}, false}]},
  Body = {natbody, #{"out" => [{var, 2, "inp"}]}},
  Lam = {lam, 3, "f", Sign, Body},
  F = [{app, 4, 1, Lam, #{"inp" => E}}],
  ?assertEqual( {ok, E}, eval( F, ?THETA0 ) ).

multiple_output_should_be_bindable_test() ->
  Sign = {sign, [{param, {name, "out1", false}, false}, {param, {name, "out2", false}, false}], []},
  E1 = [{str, "bla"}],
  E2 = [{str, "blub"}],
  Body = {natbody, #{"out1" => E1, "out2" => E2}},
  Lam = {lam, 3, "f", Sign, Body},
  F1 = [{app, 4, 1, Lam, #{}}],
  F2 = [{app, 5, 2, Lam, #{}}],
  [?assertEqual( {ok, E1}, eval( F1, ?THETA0 ) ),
   ?assertEqual( {ok, E2}, eval( F2, ?THETA0 ) )].

app_should_ignore_calling_context_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {natbody, #{"out" => [{var, 1, "x"}]}},
  Lam = {lam, 2, "f", Sign, Body},
  X = [{app, 3, 1, Lam, #{}}],
  Rho = #{"x" => [{str, "blub"}]},
  ?assertMatch( {error, {1, ?MODULE, _}}, eval( X, {Rho, fun mu/1, #{}, #{}} ) ).

app_should_hand_down_gamma_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {natbody, #{"out" => [{app, 1, 1, {var, 2, "f"}, #{}}]}},
  Lam = {lam, 3, "g", Sign, Body},
  X = [{app, 4, 1, Lam, #{}}],
  E = [{str, "blub"}],
  Gamma = #{"f" => {lam, 6, "f", Sign, {natbody, #{"out" => E}}}},
  Theta = {#{}, fun mu/1, Gamma, #{}},
  ?assertEqual( {ok, E}, eval( X, Theta ) ).

binding_should_override_body_test() ->
  F = [{str, "blub"}],
  Sign = {sign, [{param, {name, "out", false}, false}], [{param, {name, "x", false}, false}]},
  Body = {natbody, #{"x" => [{str, "bla"}], "out" => [{var, 3, "x"}]}},
  Lam = {lam, 4, "f", Sign, Body},
  X = [{app, 5, 1, Lam, #{"x" => F}}],
  ?assertEqual( {ok, F}, eval( X, ?THETA0 ) ).
  
returning_empty_list_on_nonlist_output_channel_should_fail_test() ->
  S = {sign, [{param, {name, "out", false}, false}], []},
  B = {natbody, #{"out" => []}},
  Lam = {lam, 1, "f", S, B},
  X = [{app, 2, 1, Lam, #{}}],
  ?assertMatch( {error, {1, ?MODULE, _}}, eval( X, ?THETA0 ) ).

cross_product_should_be_derivable_test() ->
  Sign = {sign, [{param, {name, "out1", false}, false}, {param, {name, "out2", false}, false}],
                [{param, {name, "p1", false}, false}, {param, {name, "p2", false}, false}]},
  E1 = [{str, "A"}, {str, "B"}],
  E2 = [{str, "1"}, {str, "2"}],
  Body = {natbody, #{"out1" => [{var, 5, "p1"}], "out2" => [{var, 6, "p2"}]}},
  Lam = {lam, 7, "f", Sign, Body},
  Binding = #{"p1" => E1, "p2" => E2},
  App1 = [{app, 8, 1, Lam, Binding}],
  App2 = [{app, 9, 2, Lam, Binding}],
  F1 = [{str, "A"}, {str, "A"}, {str, "B"}, {str, "B"}],
  F2 = [{str, "1"}, {str, "2"}, {str, "1"}, {str, "2"}],
  [?assertEqual( {ok, F1}, eval( App1, ?THETA0 ) ),
   ?assertEqual( {ok, F2}, eval( App2, ?THETA0 ) )].

dot_product_should_be_derivable1_test() ->
  Sign = {sign, [{param, {name, "out1", false}, false}, {param, {name, "out2", false}, false}],
                [{correl, [{name, "p1", false}, {name, "p2", false}]}]},
  E1 = [{str, "A"}],
  E2 = [{str, "1"}],
  Body = {natbody, #{"out1" => [{var, 5, "p1"}], "out2" => [{var, 6, "p2"}]}},
  Lam = {lam, 7, "f", Sign, Body},
  Binding = #{"p1" => E1, "p2" => E2},
  App1 = [{app, 8, 1, Lam, Binding}],
  App2 = [{app, 9, 2, Lam, Binding}],
  [?assertEqual( {ok, E1}, eval( App1, ?THETA0 ) ),
   ?assertEqual( {ok, E2}, eval( App2, ?THETA0 ) )].

dot_product_should_be_derivable2_test() ->
  Sign = {sign, [{param, {name, "out1", false}, false}, {param, {name, "out2", false}, false}],
                [{correl, [{name, "p1", false}, {name, "p2", false}]}]},
  E1 = [{str, "A"}, {str, "B"}],
  E2 = [{str, "1"}, {str, "2"}],
  Body = {natbody, #{"out1" => [{var, 5, "p1"}], "out2" => [{var, 6, "p2"}]}},
  Lam = {lam, 7, "f", Sign, Body},
  Binding = #{"p1" => E1, "p2" => E2},
  App1 = [{app, 8, 1, Lam, Binding}],
  App2 = [{app, 9, 2, Lam, Binding}],
  [?assertEqual( {ok, E1}, eval( App1, ?THETA0 ) ),
   ?assertEqual( {ok, E2}, eval( App2, ?THETA0 ) )].

dot_product_should_be_derivable3_test() ->
  Sign = {sign, [{param, {name, "out1", false}, false}, {param, {name, "out2", false}, false}],
                [{correl, [{name, "p1", false}, {name, "p2", false}]}]},
  E1 = [{str, "A"}, {str, "B"}, {str, "C"}],
  E2 = [{str, "1"}, {str, "2"}, {str, "3"}],
  Body = {natbody, #{"out1" => [{var, 5, "p1"}], "out2" => [{var, 6, "p2"}]}},
  Lam = {lam, 7, "f", Sign, Body},
  Binding = #{"p1" => E1, "p2" => E2},
  App1 = [{app, 8, 1, Lam, Binding}],
  App2 = [{app, 9, 2, Lam, Binding}],
  [?assertEqual( {ok, E1}, eval( App1, ?THETA0 ) ),
   ?assertEqual( {ok, E2}, eval( App2, ?THETA0 ) )].

aggregate_should_consume_whole_list_test() ->
  Sign = {sign, [{param, {name, "out", false}, true}],
                [{param, {name, "inp", false}, true}]},
  E1 = [{str, "A"}],
  E2 = [{str, "B"}, {str, "C"}],
  Body = {natbody, #{"out" => E1++[{var, 4, "inp"}]}},
  Lam = {lam, 5, "f", Sign, Body},
  Binding = #{"inp" => E2},
  App = [{app, 6, 1, Lam, Binding}],
  ?assertEqual( {ok, E1++E2}, eval( App, ?THETA0 ) ).

cnd_false_should_eval_else_expr_test() ->
  E = [{cnd, 1, [], [{str, "A"}], [{str, "B"}]}],
  ?assertEqual( {ok, [{str, "B"}]}, eval( E, ?THETA0 ) ).

cnd_evaluates_condition_before_decision1_test() ->
  Sign = {sign, [{param, {name, "out", false}, true}], []},
  Body = {natbody, #{"out" => []}},
  Lam = {lam, 1, "f", Sign, Body},
  App = [{app, 2, 1, Lam, #{}}],
  E = [{cnd, 3, App, [{str, "A"}], [{str, "B"}]}],
  ?assertEqual( {ok, [{str, "B"}]}, eval( E, ?THETA0 ) ).

cnd_evaluates_condition_before_decision2_test() ->
  Sign = {sign, [{param, {name, "out", false}, true}], []},
  Body = {natbody, #{"out" => [{str, "X"}]}},
  Lam = {lam, 2, "f", Sign, Body},
  App = [{app, 3, 1, Lam, #{}}],
  E = [{cnd, 4, App, [{str, "A"}], [{str, "B"}]}],
  ?assertEqual( {ok, [{str, "A"}]}, eval( E, ?THETA0 ) ).
  
cnd_evaluates_only_on_final_condition_test() ->
  Sign = {sign, [{param, {name, "out", false}, true}], []},
  Lam = {lam, 1, "f", Sign, {forbody, bash, "shalala"}},
  App = [{app, 2, 1, Lam, #{}}],
  A = [{var, 3, "a"}],
  B = [{var, 4, "b"}],
  E = [{cnd, 5, App, A, B}],
  Rho = #{"a" => [{str, "A"}], "b" => [{str, "B"}]},
  X = eval( E, {Rho, fun mu/1, #{}, #{}} ),
  ?assertMatch( {ok, [{cnd, 5, [{select, 2, 1, _}], A, B}]}, X ).

cnd_evaluates_then_expr_test() ->
  E = [{cnd, 1, [{str, "Z"}], [{var, 3, "x"}], [{str, "B"}]}],
  F = [{str, "A"}],
  Theta = {#{"x" => F}, fun mu/1, #{}, #{}},
  ?assertEqual( {ok, F}, eval( E, Theta ) ).

cnd_evaluates_else_expr_test() ->
  E = [{cnd, 1, [], [{str, "B"}], [{var, 3, "x"}]}],
  F = [{str, "A"}],
  Theta = {#{"x" => F}, fun mu/1, #{}, #{}},
  ?assertEqual( {ok, F}, eval( E, Theta ) ).

foreign_app_with_cnd_param_is_left_untouched_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], [{param, {name, "p", false}, false}]},
  Lam = {lam, 1, "f", Sign, {forbody, bash, "shalala"}},
  App1 = [{app, 2, 1, Lam, #{"p" => [{str, "A"}]}}],
  E = [{cnd, 4, App1, [], []}],
  App2 = [{app, 5, 1, Lam, #{"p" => E}}],
  X = eval( App2, ?THETA0 ),
  ?assertMatch( {ok, [{app, 5, 1, Lam, _}]}, X ).

foreign_app_with_select_param_is_left_untouched_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}],
                [{param, {name, "p", false}, false}]},
  Lam = {lam, 1, "f", Sign, {forbody, bash, "shalala"}},
  App1 = [{app, 2, 1, Lam, #{"p" => [{str, "A"}]}}],
  App2 = [{app, 4, 1, Lam, #{"p" => App1}}],
  X = eval( App2, ?THETA0 ),
  ?assertMatch( {ok, [{app, 4, 1, Lam, _}]}, X ).

app_non_final_result_preserves_app_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Select = [{select, 1, 1, {fut, "f", 1234, [{param, {name, "out", false}, false}]}}],
  Cnd = [{cnd, 1, Select, [{var, 2, "x"}], [{var, 3, "y"}]}],
  Body = {natbody, #{"out" => Cnd}},
  Lam = {lam, 3, "g", Sign, Body},
  App = [{app, 4, 1, Lam, #{}}],
  X = eval( App, ?THETA0 ),
  ?assertMatch( {ok, App}, X ).

app_tail_recursion_is_optimized_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Select = [{select, 1, 1, {fut, "f", 1234, [{param, {name, "out", false}, false}]}}],
  Body = {natbody, #{"out" => Select}},
  Lam = {lam, 3, "g", Sign, Body},
  App = [{app, 4, 1, Lam, #{}}],
  X = eval( App, ?THETA0 ),
  ?assertMatch( {ok, Select}, X ).

app_non_final_result_preserves_app_with_new_lam_test() ->
  CSign = {sign, [{param, {name, "out", false}, true}], []},
  CLam = {lam, 1, "f", CSign, {forbody, bash, "shalala"}},
  CApp = [{app, 2, 1, CLam, #{}}],
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Body = {natbody, #{"out" => [{cnd, 3, CApp, [{var, 4, "x"}], [{var, 5, "x"}]}],
                     "x" => [{str, "A"}]}},
  Lam = {lam, 7, "f", Sign, Body},
  App = [{app, 8, 1, Lam, #{}}],
  {ok, X} = eval( App, ?THETA0 ),
  [{app, 8, 1, {lam, 7, "f", Sign, {natbody, BodyMap1}}, #{}}] = X,
  Val = maps:get( "out", BodyMap1 ),
  ?assertMatch( [{cnd, 3, [{select, 2, 1, _}], [{var, 4, "x"}], [{var, 5, "x"}]}], Val ).

nested_app_undergoes_reduction_test() ->
  Sign = {sign, [{param, {name, "out", false}, false}], []},
  Lam1 = {lam, 1, "f", Sign, {forbody, bash, "shalala"}},
  App1 = [{app, 2, 1, Lam1, #{}}],
  Body2 = {natbody, #{"out" => App1}},
  Lam2 = {lam, 3, "g", Sign, Body2},
  App2 = [{app, 4, 1, Lam2, #{}}],
  {ok, X} = eval( App2, ?THETA0 ),
  [{select, 2, 1, {fut, "f", R, _}}] = X,
  Omega = #{{"out", R} => [{str, "A"}]},
  Y = eval( X, {#{}, fun mu/1, #{}, Omega} ),
  ?assertEqual( {ok, [{str, "A"}]}, Y ).

app_select_param_is_enumerated_test() ->
  Sign1 = {sign, [{param, {name, "out", false}, false}], []},
  Lam1 = {lam, 1, "f", Sign1, {forbody, bash, "shalala"}},
  Sign2 = {sign, [{param, {name, "out", false}, false}],
                 [{param, {name, "inp", false}, false}]},
  Body2 = {natbody, #{"out" => [{var, 2, "inp"}]}},
  Lam2 = {lam, 3, "g", Sign2, Body2},
  A0 = {app, 4, 1, Lam1, #{}},
  A = [A0, A0],
  B = [{app, 5, 1, Lam2, #{"inp" => A}}],
  X = eval( B, ?THETA0 ), 
  ?assertMatch( {ok, [{select, 4, 1, _}, {select, 4, 1, _}]}, X ).

app_str_is_evaluated_while_select_remains_test() ->
  Sign2 = {sign, [{param, {name, "out", false}, false}],
                 [{param, {name, "inp", false}, false}]},
  Body2 = {forbody, bash, "lalala"},
  Lam2 = {lam, 2, "g", Sign2, Body2},
  Fut = {fut, "h", 1234, [{param, {name, "y", false}, false}]},
  Select = {select, 4, 1, Fut},
  Str = {str, "blub"},
  A = [Str, Select],
  B = [{app, 3, 1, Lam2, #{"inp" => A}}],
  X = eval( B, ?THETA0 ), 
  ?assertMatch( {ok, [{select, 3, 1, {fut, "g", _, [{param, {name, "out", false}, false}]}},
                      {app, 3, 1, Lam2, #{"inp" := [Select]}}]}, X ).

% deftask find_clusters( cls( File ) : state( File ) ) {
%   cls = state;
% }
% mu0 = 1;
% cls = find_clusters( state: mu0 );
% cls;
identity_fn_should_resolve_var_test() ->
  Lo = [{param, {name, "cls", false}, false}],
  Li = [{param, {name, "state", false}, false}],
  Sign = {sign, Lo, Li},
  Body = {natbody, #{"cls" => [{var, 1, "state"}]}},
  Lam = {lam, 2, "find_clusters", Sign, Body},
  Fa = #{"state" => [{var, 3, "mu0"}]},
  App = {app, 4, 1, Lam, Fa},
  Rho = #{"cls" => [App], "mu0" => [{str, "1"}]},
  X = [{var, 6, "cls"}],
  ?assertEqual( {ok, [{str, "1"}]}, eval( X, {Rho, fun sem:mu/1, #{}, #{}} ) ).


% deftask bwa-mem( outbam( File ) : fastq( File ) )in bash *{
%   ...
% }*
%
% deftask compareToCTRL( somaticVCF( File ) : <tumorParts( False )> ctrlBam( File ) ) in bash *{
%   ...
% }*
%
% deftask getVariants( somaticVCF( File ) : tumor( File ), <ctrlBam( File )> ) {
% 
%   tumorBam   = bwa-mem( fastq: tumor );
%   somaticVCF = compareToCTRL( ctrlBam: ctrlBam, tumorParts: tumorBam );
% }
%
% ctrlFq = "ctrl.fq"
% tumorFq = "tumor.fq"
%
% ctrlBam    = bwa-mem( fastq: ctrlFq );
% somaticVCF = getVariants( ctrlBam: ctrlBam, tumor: tumorFq );
%
% somaticVCF;

christopher_test() ->
  X = [{app,10,1,
              {lam,20,"getVariants",{sign,[{param,{name,"somaticVCF",true},false}],
                         [{param,{name,"tumor",true},false},
                          {param,{name,"ctrlBam",true},true}]},
                   {natbody,#{"somaticVCF" => [{app,30,1,
                                    {var,40,"compareToCTRL"},
                                    #{"ctrlBam"    => [{var,50,"ctrlBam"}],
                                      "tumorParts" => [{var,60,"tumorBam"}]}}],
                              "tumorBam" => [{app,70,1,
                                    {var,80,"bwa-mem"},
                                    #{"fastq" => [{var,90,"tumor"}]}}]}}},
              #{"ctrlBam" => [{select,100,1,{fut,"bwa-mem",1,[{param,{name,"bamout",true},false}]}}],
                "tumor" => [{str,"data/CH_JK_001/CH_JK_001_R1_001.fastq.gz"}]}}],

  Gamma = #{"compareToCTRL" => {lam, 120, "compareToCTRL", {sign, [{param, {name, "somaticVCF", true}, false}],
                                            [{param, {name, "tumorParts", true}, true},
                                             {param, {name, "ctrlBam", true}, false}]},
                                     {forbody, bash, "blub"}},
            "bwa-mem" => {lam, 130, "bwa-mem", {sign, [{param, {name, "bamout", true}, false}],
                                      [{param, {name, "fastq", true}, false}]},
                                {forbody, bash, "bla"}}
                                     },
  Theta = {#{}, fun mu/1, Gamma, #{}},


  ?assertMatch(
    {ok, [{app,30,1,
               {lam,120,"compareToCTRL",{sign,[{param,{name,"somaticVCF",true},false}],
                          [{param,{name,"tumorParts",true},true},{param,{name,"ctrlBam",true},false}]},
                    {forbody,bash,"blub"}},
               #{"ctrlBam" := [{select,100,1,{fut,"bwa-mem",1,[{param,{name,"bamout",true},false}]}}],
                 "tumorParts" := [{select,70,1,{fut,"bwa-mem",_,[{param,{name,"bamout",true},false}]}}]}}]},
    eval( X, Theta ) ).



% deftask bowtie2-align( sam( File ) : [fastq1( File ) fastq2( File )] )in bash *{
%   sam=bowtie2.sam
%   tar xf $idx
%   bowtie2 -D 5 -R 1 -N 0 -L 22 -i S,0,2.50 \
%   -p 1 \
%   --no-unal -x bt2idx -1 $fastq1 -2 $fastq2 -S $sam
%   rm bt2idx.*
% }*
%
% deftask per-fastq( sam : [fastq1( File ) fastq2( File )] ) {
%   sam = bowtie2-align(
%     fastq1: fastq1,
%     fastq2: fastq2 );
% }
%
% fastq1 = "SRR359188_1.filt.fastq";
% fastq2 = "SRR359188_2.filt.fastq";
%
% sam = per-fastq(
%   fastq1: fastq1,
%   fastq2: fastq2 );
%
% sam;

marc_test() ->

  X = [{var,10,"sam"}],

  Rho = #{"fastq2"=>[{str,"SRR359188_2.filt.fastq"}],
          "sam"   =>[{app,20,1,{var,30,"per-fastq"},
                        #{"fastq2"=>[{var,40,"fastq2"}],
                          "fastq1"=>[{var,50,"fastq1"}]}}],
          "fastq1"=>[{str,"SRR359188_1.filt.fastq"}]
         },

  Gamma = #{"bowtie2-align"=>{lam,60,"bowtie2-align",{sign,[{param,{name,"sam",true},false}],
                                      [{correl,[{name,"fastq1",true},{name,"fastq2",true}]}]},
                                {forbody,bash,"blub"}},
            "per-fastq"    =>{lam,70,"per-fastq",{sign,[{param,{name,"sam",true},false}],
                                      [{correl,[{name,"fastq1",true},{name,"fastq2",true}]}]},
                                {natbody,#{"sam"=>[{app,80,1,{var,90,"bowtie2-align"},
                                                        #{"fastq2"=>[{var,100,"fastq2"}],
                                                          "fastq1"=>[{var,110,"fastq1"}]}}]}}}
           },

  Theta = {Rho, fun mu/1, Gamma, #{}},

  ?assertMatch( {ok, [{select,80,1,{fut,"bowtie2-align",_,[{param,{name,"sam",true},false}]}}]}, eval( X, Theta ) ).
  


-endif.