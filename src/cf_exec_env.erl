-module( cf_exec_env ).

%% =========================================================
%% Callback function declarations
%% =========================================================

-callback submit( Msg::tuple(), {Mod::atom(), Ref::pid()} ) -> ok.
