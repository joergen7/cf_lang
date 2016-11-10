-module( cf_lang_app ).

-behaviour( application ).

-export( [start/2, stop/1] ).

%%====================================================================
%% API
%%====================================================================

start( _StartType, _StartArgs ) ->
  cf_lang_sup:start_link().

stop( _State ) ->
  ok.

%%====================================================================
%% Internal functions
%%====================================================================
