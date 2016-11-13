-record( reply_ok, {tag, id, result_map} ).
-record( reply_error, {tag, id, app_line, lam_name, script, output} ).
-record( workflow, {tag, lang, content} ).
-record( halt_ok, {tag, result} ).
-record( halt_error_workflow, {tag, line, module, reason} ).
-record( halt_error_task, {tag, id, app_line, lam_name, script, output} ).
-record( submit, {tag, id, app_line, lam_name, out_vars, in_vars, lang, script,
                  arg_map} ).