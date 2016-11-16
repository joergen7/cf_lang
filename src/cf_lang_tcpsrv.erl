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

-module( cf_lang_tcpsrv ).
-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

-define( TCP_PORT, 17489 ).
-define( LISTEN_OPT, [binary, {packet, 4}, {reuseaddr, true}, {active, true}] ).

-export( [start_link/0] ).

%%==========================================================
%% API functions
%%==========================================================

start_link() ->
  io:format( "Starting server.~n" ),
  case gen_tcp:listen( ?TCP_PORT, ?LISTEN_OPT ) of
    {error, Reason}    -> io:format( "~p~n", [Reason] ), error( Reason );
    {ok, ListenSocket} ->
      {ok, spawn_link( fun() -> listen_loop( ListenSocket ) end )}
  end.

%%==========================================================
%% Internal Functions
%%==========================================================

listen_loop( ListenSocket ) ->
  process_flag( trap_exit, true ),
  case gen_tcp:accept( ListenSocket, 500 ) of
    {error, timeout} ->
      receive
        _ ->
          io:format( "Closing listen socket.~n" ),
          gen_tcp:close( ListenSocket )
      after 0 ->
        listen_loop( ListenSocket )
      end;
    {error, Reason}  -> error( Reason );
    {ok, Socket}     ->
      {ok, Child} = cf_lang_sup:start_tcpenv( Socket ),
      case gen_tcp:controlling_process( Socket, Child ) of
        {error, Reason} -> error( Reason );
        ok              -> listen_loop( ListenSocket )
      end
  end.

