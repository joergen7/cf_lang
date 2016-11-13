-module( cf_exec_env ).

-include( "cf_lang.hrl" ).

%% =========================================================
%% Callback function declarations
%% =========================================================

-callback submit( Submit::#submit{}, {Mod::atom(), Ref::pid()} ) -> ok.
