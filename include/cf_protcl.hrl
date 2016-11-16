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

-record( reply_ok, {id, result_map} ).
-record( reply_error, {id, app_line, lam_name, script, output} ).
-record( workflow, {lang, content, suppl} ).
-record( halt_ok, {result} ).
-record( halt_eworkflow, {line, module, reason} ).
-record( halt_etask, {id, app_line, lam_name, script, output} ).
-record( submit, {id, app_line, lam_name, out_vars, in_vars, lang, script,
                  arg_map, suppl} ).