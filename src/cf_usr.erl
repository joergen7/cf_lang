-module( cf_usr ).

-include( "cf_lang.hrl" ).

%% =========================================================
%% Callback function declarations
%% =========================================================

-callback halt( Halt::#halt_ok{} | #halt_error_task{} | #halt_error_workflow{},
                {Mod::atom(), Ref::pid()} ) -> ok.
