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


%% =============================================================================
%% Symbol Declaration
%% =============================================================================

Nonterminals
  script stat assign exprlist expr binding assignlist sign query app cnd
  paramlist param inparamlist inparam namelist name defun lang bindinglist
  compoundexpr.

Terminals
  intlit strlit body bash beginif colon comma deftask else endif eq file in
  python r lbrace lparen lsquarebr ltag nil rbrace rparen rsquarebr rtag
  semicolon string then id perl.


%% =============================================================================
%% Syntax Definition
%% =============================================================================

Rootsymbol script.


script       -> stat        : '$1'.
script       -> stat script : combine( '$1', '$2' ).

stat         -> query  : {'$1', #{}, #{}}.
stat         -> assign : {undef, '$1', #{}}.
stat         -> defun  : {undef, #{}, '$1'}.

query        -> compoundexpr semicolon : '$1'.

assign       -> exprlist eq compoundexpr semicolon : mk_assign( get_line( '$2' ), '$1', '$3', 1 ).

assignlist   -> assign            : '$1'.
assignlist   -> assign assignlist : maps:merge( '$1', '$2' ).

defun        -> deftask id sign lbrace assignlist rbrace : mk_natlam( '$1', '$2', '$3', '$5' ).
defun        -> deftask id sign in lang body             : mk_forlam( '$1', '$2', '$3', '$5', '$6' ).

lang         -> bash   : bash.
lang         -> perl   : perl.
lang         -> python : python.
lang         -> r      : r.

compoundexpr -> nil      : [].
compoundexpr -> exprlist : '$1'.

expr         -> intlit : mk_str( '$1' ).
expr         -> strlit : mk_str( '$1' ).
expr         -> id     : mk_var( '$1' ).
expr         -> cnd    : '$1'.
expr         -> app    : '$1'.

exprlist     -> expr          : ['$1'].
exprlist     -> expr exprlist : ['$1'|'$2'].

cnd          -> beginif compoundexpr then compoundexpr
                else compoundexpr endif                : {cnd, get_line( '$1' ), '$2', '$4', '$6'}.

app          -> id lparen rparen             : {app, get_line( '$1' ), 1, mk_var( '$1' ), #{}}.
app          -> id lparen bindinglist rparen : {app, get_line( '$1' ), 1, mk_var( '$1' ), '$3'}.

binding      -> id colon compoundexpr : mk_binding( '$1', '$3' ).

bindinglist  -> binding                   : '$1'.
bindinglist  -> binding comma bindinglist : maps:merge( '$1', '$3' ).

sign         -> lparen paramlist colon rparen             : {sign, '$2', []}.
sign         -> lparen paramlist colon inparamlist rparen : {sign, '$2', '$4'}.

inparam      -> param                        : '$1'.
inparam      -> lsquarebr namelist rsquarebr : {correl, '$2'}.

inparamlist  -> inparam             : ['$1'].
inparamlist  -> inparam inparamlist : ['$1'|'$2'].

param        -> name           : {param, '$1', false}.
param        -> ltag name rtag : {param, '$2', true}.

paramlist    -> param           : ['$1'].
paramlist    -> param paramlist : ['$1'|'$2'].

name         -> id                      : {name, get_name( '$1' ), false}.
name         -> id lparen string rparen : {name, get_name( '$1' ), false}.
name         -> id lparen file rparen   : {name, get_name( '$1' ), true}.

namelist     -> name          : ['$1'].
namelist     -> name namelist : ['$1'|'$2'].

%% =============================================================================
%% Erlang Code
%% =============================================================================

Erlang code.

-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

-export( [string/1, file/1] ).

-ifdef( TEST ).
-include_lib( "eunit/include/eunit.hrl" ).
-endif.

string( S ) ->
  case cf_scan:string( S ) of
    {error, {Line, cf_scan, ErrorInfo}, _} ->
      ErrorMsg = lists:flatten( format_error( ErrorInfo ) ),
      {error, {Line, cf_scan, ErrorMsg}};
    {ok, TokenLst, _} ->
    try parse( TokenLst ) of
      Ret -> Ret
    catch
      throw:E -> {error, E}
    end
  end.

file( Filename ) ->
  case file:read_file( Filename ) of
    {error, Reason} -> {error, Reason};
    {ok, B}         ->
      S = binary_to_list( B ),
      string( S )
  end.



combine( {Target1, Rho1, Global1}, {Target2, Rho2, Global2} ) ->
  case Target1 of
    undef -> {Target2, maps:merge( Rho1, Rho2 ), maps:merge( Global1, Global2 )};
    _     ->
      case Target2 of
        undef -> {undef, maps:merge( Rho1, Rho2 ), maps:merge( Global1, Global2 )};
        _     -> {Target1++Target2, maps:merge( Rho1, Rho2 ), maps:merge( Global1, Global2 )}
      end
  end.

mk_var( {id, Line, Name} ) -> {var, Line, Name}.

mk_str( {intlit, _Line, N} ) -> {str, N};
mk_str( {strlit, _Line, S} ) -> {str, S}.

get_line( {_, Line, _} ) -> Line.

get_name( {id, _Line, Name} ) -> Name.

mk_binding( {id, _, Name}, ExprList ) ->
  #{Name => ExprList}.

mk_assign( _Line, [], _ExprList, _Channel ) -> #{};

mk_assign( Line, [{var, _Line, Name}|Rest], ExprList, Channel ) ->
  Rho = mk_assign( Line, Rest, ExprList, Channel+1 ),
  Value = lists:flatmap( fun( E ) -> set_channel( Line, E, Channel ) end, ExprList ),
  Rho#{Name => Value};

mk_assign( Line, [_E|_Rest], _ExprList, _Channel ) ->
  throw( {Line, ?MODULE, "non-variable expression on left-hand side of assignment"} ).


set_channel( _Line, {app, AppLine, _Channel, LamList, Binding}, N ) -> [{app, AppLine, N, LamList, Binding}];
set_channel( _Line, E, 1 ) -> [E];
set_channel( Line, _, _ )  -> throw( {Line, ?MODULE, "multiple value bind on non-application expression"} ).

mk_natlam( {deftask, Line, _}, {id, _, Name}, Sign, Block ) ->
  #{Name => {lam, Line, Name, Sign, {natbody, Block}}}.

mk_forlam( {deftask, Line, _}, {id, _, Name}, Sign, Lang, {body, _, Code} ) ->
  #{Name => {lam, Line, Name, Sign, {forbody, Lang, Code}}}.

%% =============================================================================
%% Unit Tests
%% =============================================================================

-ifdef( TEST ).

nil_should_be_recognized_test() ->
  ?assertEqual( {ok, {[], #{}, #{}}}, string( "nil;" ) ).

var_should_be_recognized_test() ->
  ?assertEqual( {ok, {[{var, 1, "blub"}], #{}, #{}}}, string( "blub;" ) ).

multi_element_compoundexpr_should_be_recognized_test() ->
  ?assertEqual( {ok, {[{var, 1, "bla"}, {var, 1, "blub"}], #{}, #{}}},
                 string( "bla blub;" ) ).

multiple_queries_should_be_joined_test() ->
  ?assertEqual( {ok, {[{var, 1, "bla"}, {var, 1, "blub"}], #{}, #{}}},
                 string( "bla; blub;" ) ).

strlit_should_be_recognized_test() ->
  ?assertEqual( {ok, {[{str, "bla"}], #{}, #{}}}, string( "\"bla\";" ) ).

intlit_should_be_recognized_test() ->
  ?assertEqual( {ok, {[{str, "-5"}], #{}, #{}}}, string( "-5;" ) ).

cnd_should_be_recognized_test() ->
  ?assertEqual( {ok, {[{cnd, 1, [], [{str, "bla"}], [{str, "blub"}]}],
                  #{}, #{}}},
                 string( "if nil then \"bla\" else \"blub\" end;" ) ).

app_should_be_recognized_test() ->
  [?assertEqual( {ok, {[{app, 1, 1, {var, 1, "f"}, #{}}], #{}, #{}}},
                  string( "f();" ) ),
   ?assertEqual( {ok, {[{app, 1, 1, {var, 1, "f"}, #{"x" => [{var, 1, "x"}]}}],
                   #{}, #{}}}, string( "f( x: x );" ) ),
   ?assertEqual( {ok, {[{app, 1, 1, {var, 1, "f"},
                     #{"x" => [{var, 1, "x"}],
                       "y" => [{str, "y"}]}}], #{}, #{}}},
                  string( "f( x: x, y: \"y\" );" ) )].


assign_should_be_recognized_test() ->
  [?assertEqual( {ok, {undef, #{"x" => [{str, "x"}]}, #{}}},
                      string( "x = \"x\";" ) ),
   ?assertEqual( {ok, {undef, #{"x" => [{app, 1, 1, {var, 1, "f"}, #{}}],
                                "y" => [{app, 1, 2, {var, 1, "f"}, #{}}]},
                              #{}}}, string( "x y = f();" ) ),
   ?assertMatch( {error, {1, cf_parse, _}}, string( "x y = \"A\";" ) ),
   ?assertMatch( {error, {1, cf_parse, _}}, string( "\"a\" = \"A\";" ) )].

native_deftask_should_be_recognized_test() ->
  ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                           {sign, [{param, {name, "out", false}, false}],
                                                  []},
                                           {natbody, #{"out" => [{str, "A"}]}}}}}},
                 string( "deftask f( out : ) { out = \"A\"; }" ) ).

foreign_deftask_should_be_recognized_test() ->
  [?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   []},
                                            {forbody, bash, "out=A"}}}}},
                  string( "deftask f( out : )in bash *{out=A}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   []},
                                            {forbody, r, "out=\"A\""}}}}},
                  string( "deftask f( out : )in R *{out=\"A\"}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   []},
                                            {forbody, python, ""}}}}},
                  string( "deftask f( out : )in python *{}*" ) )].

sign_with_inparam_should_be_recognized_test() ->
  [?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   [{param, {name, "inp", false}, false}]},
                                            {forbody, python, "(defparameter out \"A\")"}}}}},
                  string( "deftask f( out : inp )in python *{(defparameter out \"A\")}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   [{param, {name, "a", false}, false},
                                                    {param, {name, "b", false}, false}]},
                                            {forbody, python, "(defparameter out \"A\")"}}}}},
                  string( "deftask f( out : a b )in python *{(defparameter out \"A\")}*" ) )].

param_should_be_recognized_test() ->
  [?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   [{param, {name, "inp", false}, false}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( out( String ) : inp( String ) )in bash *{blub}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", true}, false}],
                                                   [{param, {name, "inp", true}, false}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( out( File ) : inp( File ) )in bash *{blub}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, true}],
                                                   [{param, {name, "inp", false}, true}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( <out> : <inp> )in bash *{blub}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, true}],
                                                   [{param, {name, "inp", false}, true}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( <out( String )> : <inp( String )> )in bash *{blub}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", true}, true}],
                                                   [{param, {name, "inp", true}, true}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( <out( File )> : <inp( File )> )in bash *{blub}*" ) )].

correl_inparam_should_be_recognized_test() ->
  [?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   [{correl, [{name, "a", true},
                                                              {name, "b", true}]}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( out : [a( File ) b( File )] )in bash *{blub}*" ) ),
   ?assertEqual( {ok, {undef, #{}, #{"f" => {lam, 1, "f",
                                            {sign, [{param, {name, "out", false}, false}],
                                                   [{correl, [{name, "a", true},
                                                              {name, "b", true}]},
                                                    {param, {name, "c", false}, false}]},
                                            {forbody, bash, "blub"}}}}},
                  string( "deftask f( out : [a( File ) b( File )] c )in bash *{blub}*" ) )].

-endif.


