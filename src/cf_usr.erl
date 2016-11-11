-module( cf_usr ).

%% =========================================================
%% Callback function declarations
%% =========================================================

-callback halt( Msg::tuple(), {Mod::atom(), Ref::pid()} ) -> ok.
