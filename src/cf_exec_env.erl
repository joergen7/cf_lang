-module( cf_exec_env ).

%% =========================================================
%% Callback function declarations
%% =========================================================

-callback submit( R::binary(), App::tuple(), {Mod::atom(), Ref::pid()} ) -> ok.
