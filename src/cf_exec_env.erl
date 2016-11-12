-module( cf_exec_env ).

%% =========================================================
%% Callback function declarations
%% =========================================================

-callback submit( App::tuple(), {Mod::atom(), Ref::pid()} ) -> ok.
