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

-module( cf_protcl ).
-author( "Jörgen Brandt <brandjoe@hu-berlin.de>" ).

-export( [encode/1, decode/2] ).

-include( "cf_protcl.hrl" ).

-define( PROTOCOL, <<"cf_protcl">> ).
-define( VSN, <<"0.1.0">> ).


encode( #halt_ok{ result=Result } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => halt_ok,
                   data     => #{ result => Result }
                 } );

encode( #halt_eworkflow{ line   = Line,
                         module = Module,
                         reason = Reason} ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => halt_eworkflow,
                   data     => #{ line   => Line,
                                  module => Module,
                                  reason => Reason
                                }
                 } );

encode( #halt_etask{ id       = R,
                     app_line = AppLine,
                     lam_name = LamName,
                     script   = Script,
                     output   = Output } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => halt_etask,
                   data     => #{
                                 id       => R,
                                 app_line => AppLine,
                                 lam_name => LamName,
                                 script   => Script,
                                 output   => Output
                                }
                 } );


encode( #submit{ id       = R,
                 app_line = AppLine,
                 lam_name = LamName,
                 out_vars = OutVars,
                 in_vars  = InVars,
                 lang     = Lang,
                 script   = Script,
                 arg_map  = ArgMap,
                 suppl    = Suppl } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => submit,
                   data     => #{ id       => R,
                                  app_line => AppLine,
                                  lam_name => LamName,
                                  out_vars => OutVars,
                                  in_vars  => InVars,
                                  lang     => Lang,
                                  script   => Script,
                                  arg_map  => ArgMap,
                                  suppl    => Suppl
                                }
                 } );


encode( #reply_ok{ id = Id, result_map = ResultMap } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => reply_ok,
                   data     => #{ id         => Id,
                                  result_map => ResultMap
                                }
                 } );

encode( #reply_error{ id = Id,
                      app_line = AppLine,
                      lam_name = LamName,
                      script   = Script,
                      output   = Output
                    } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   msg_type => reply_error,
                   data     => #{ id       => Id,
                                  app_line => AppLine,
                                  lam_name => LamName,
                                  script   => Script,
                                  output   => Output
                                }
                 } ).


decode( workflow, B ) ->
  #{ <<"protocol">> := ?PROTOCOL,
     <<"vsn">>      := ?VSN,
     <<"msg_type">> := <<"workflow">>,
     <<"data">>     := #{ <<"lang">>    := <<"cuneiform">>,
                          <<"content">> := Content,
                          <<"suppl">>   := Suppl
                        }
  } = jsone:decode( B ),

  #workflow{ lang=cuneiform, content=Content, suppl=Suppl };

decode( reply, B ) ->

  case jsone:decode( B ) of

    #{ <<"protocol">> := ?PROTOCOL,
       <<"vsn">>      := ?VSN,
       <<"msg_type">> := <<"reply_ok">>,
       <<"data">>     := #{ <<"id">>         := R,
                            <<"result_map">> := ResultMap
                          }
     } -> #reply_ok{ id=R, result_map=ResultMap };

    #{ <<"protocol">> := ?PROTOCOL,
       <<"vsn">>      := ?VSN,
       <<"msg_type">> := <<"reply_error">>,
       <<"data">>     := #{ <<"id">>       := R,
                            <<"output">>   := Output,
                            <<"app_line">> := AppLine,
                            <<"lam_name">> := LamName,
                            <<"script">>   := Script
                          }
    } -> #reply_error{ id       = R,
                       app_line = AppLine,
                       lam_name = LamName,
                       script   = Script,
                       output   = Output }
  end;

decode( submit, B ) ->
  #{ <<"protocol">> := ?PROTOCOL,
     <<"vsn">>      := ?VSN,
     <<"msg_type">> := <<"submit">>,
     <<"data">>     := #{ <<"id">>       := Id,
                          <<"app_line">> := AppLine,
                          <<"lam_name">> := LamName,
                          <<"out_vars">> := OutVars0,
                          <<"in_vars">>  := InVars0,
                          <<"lang">>     := Lang0,
                          <<"script">>   := Script,
                          <<"arg_map">>  := ArgMap,
                          <<"suppl">>    := Suppl
                        }
  } = jsone:decode( B ),

  F = fun( <<"is_file">>, P, Acc )  -> Acc#{ is_file => P };
         ( <<"is_list">>, P, Acc )  -> Acc#{ is_list => P };
         ( <<"name">>, N, Acc )              -> Acc#{ name => N }
      end,

  OutVars = [maps:fold( F, #{}, O ) || O <- OutVars0],
  InVars  = [maps:fold( F, #{}, I ) || I <- InVars0],
  Lang    = binary_to_atom( Lang0, utf8 ),

  #submit{ id       = Id,
           app_line = AppLine,
           lam_name = LamName,
           out_vars = OutVars,
           in_vars  = InVars,
           lang     = Lang,
           script   = Script,
           arg_map  = ArgMap,
           suppl    = Suppl
         }.