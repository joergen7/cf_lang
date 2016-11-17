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

-module( cf_lang_sup ).
-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

-behaviour( supervisor ).

-export( [start_link/0, start_tcpenv/1, start_session/3] ).
-export( [init/1] ).

-define( SERVER, ?MODULE ).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link( {local, ?SERVER}, ?MODULE, [] ).

start_tcpenv( Socket ) ->
  ChildSpec = #{
                id      => {cf_lang_tcpenv,
                            erlang:unique_integer( [positive, monotonic] )},
                start   => {cf_lang_tcpenv, start_link, [Socket]},
                restart => temporary,
                type    => worker
               },
  supervisor:start_child( ?SERVER, ChildSpec ).

start_session( Query, Rho, Gamma ) ->
  ChildSpec = #{
                id      => {cf_lang_session,
                            erlang:unique_integer( [positive, monotonic] )},
                start   => {cf_lang_session, start_link, [Query, Rho, Gamma]},
                restart => temporary,
                type    => worker
               },
  supervisor:start_child( ?SERVER, ChildSpec ).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init( [] ) ->

    TcpSrv = #{
               id      => cf_lang_tcpsrv,
               start   => {cf_lang_tcpsrv, start_link, []},
               restart => permanent,
               type    => worker
              },

    {ok, { {one_for_one, 10, 5}, [TcpSrv]} }.

%%====================================================================
%% Internal functions
%%====================================================================
