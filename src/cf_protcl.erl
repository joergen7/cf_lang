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


encode( #halt_ok{ tag    = Tag,
                  result = Result } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt_ok,
                   data     => #{ result => Result }
                 } );

encode( #halt_eworkflow{ tag    = Tag,
                              line   = Line,
                              module = Module,
                              reason = Reason} ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt_eworkflow,
                   data     => #{ line   => Line,
                                  module => Module,
                                  reason => Reason
                                }
                 } );

encode( #halt_etask{ tag      = Tag,
                     id       = R,
                     app_line = AppLine,
                     lam_name = LamName,
                     script   = Script,
                     output   = Output } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => halt_etask,
                   data     => #{
                                 id       => R,
                                 app_line => AppLine,
                                 lam_name => LamName,
                                 script   => Script,
                                 output   => Output
                                }
                 } );


encode( #submit{ tag      = Tag,
	             suppl    = Suppl,
                 id       = R,
                 app_line = AppLine,
                 lam_name = LamName,
                 out_vars = OutVars,
                 in_vars  = InVars,
                 lang     = Lang,
                 script   = Script,
                 arg_map  = ArgMap } ) ->

  jsone:encode( #{ protocol => ?PROTOCOL,
                   vsn      => ?VSN,
                   tag      => Tag,
                   msg_type => submit,
                   data     => #{ suppl    => Suppl,
                                  id       => R,
                                  app_line => AppLine,
                                  lam_name => LamName,
                                  out_vars => OutVars,
                                  in_vars  => InVars,
                                  lang     => Lang,
                                  script   => Script,
                                  arg_map  => ArgMap
                                }
                 } ).





decode( workflow, B ) ->
  #{ <<"protocol">> := ?PROTOCOL,
     <<"vsn">>      := ?VSN,
     <<"tag">>      := Tag,
     <<"msg_type">> := <<"workflow">>,
     <<"data">>     := #{ <<"lang">>    := <<"cuneiform">>,
                          <<"content">> := Content,
                          <<"suppl">>   := Suppl
                        }
  } = jsone:decode( B ),

  #workflow{ tag=Tag, suppl=Suppl, lang=cuneiform, content=Content };

decode( reply, B ) ->

  case jsone:decode( B ) of

    #{ <<"protocol">> := ?PROTOCOL,
       <<"vsn">>      := ?VSN,
       <<"tag">>      := Tag,
       <<"msg_type">> := <<"reply_ok">>,
       <<"data">>     := #{ <<"id">>         := R,
                            <<"result_map">> := ResultMap
                          }
     } -> #reply_ok{ tag=Tag, id=R, result_map=ResultMap };

    #{ <<"protocol">> := ?PROTOCOL,
       <<"vsn">>      := ?VSN,
       <<"tag">>      := Tag,
       <<"msg_type">> := <<"reply_error">>,
       <<"data">>     := #{ <<"id">>         := R,
                            <<"output">>     := Output,
                            <<"app_line">>   := AppLine,
                            <<"lam_name">> := LamName,
                            <<"script">>     := Script
                          }
    } -> #reply_error{ tag      = Tag,
                       id       = R,
                       app_line = AppLine,
                       lam_name = LamName,
                       script   = Script,
                       output   = Output }
  end.

