# exec_command_ef_ev

## Location
src/bin/psql/command.c: 1177 - 1292

## Overview
Implements the \ef (edit function) and \ev (edit view) commands in psql for editing existing functions/views or creating new ones from templates using an external editor.

## Definition
```c
static backslashResult exec_command_ef_ev(PsqlScanState scan_state, bool active_branch, PQExpBuffer query_buf, bool is_func)
```

## Detailed Description
This function provides advanced editing capabilities for PostgreSQL functions and views through the \ef and \ev commands. When given a function or view name, it retrieves the complete definition from the database and loads it into the editor. When called without arguments, it provides a template for creating new functions or views. The function handles sophisticated line number processing for functions, automatically adjusting line numbers to correspond to the function body rather than the complete definition. It supports the syntax object_name:line_number for precise cursor positioning.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing the complete object specification (potentially including line number)
- `active_branch`: Boolean indicating whether this command should be executed or ignored due to conditional logic
- `query_buf`: PQExpBuffer that will contain the function/view definition for editing
- `is_func`: Boolean flag distinguishing between \ef (true) and \ev (false) commands

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - strip_lineno_from_objdesc
  - resetPQExpBuffer, appendPQExpBufferStr
  - lookup_object_oid
  - get_create_object_cmd
  - do_edit
  - ignore_slash_whole_line
  - pg_log_error, strncmp, strchr
- Called from (representative examples):
  - exec_command (for both \ef and \ev commands)

## Notes and Other Information
- Supports syntax: \ef function_name[:line_number] or \ev view_name[:line_number]
- Without arguments, provides CREATE FUNCTION or CREATE VIEW templates
- For functions, intelligently adjusts line numbers to point to function body (after AS, BEGIN, or RETURN keywords)
- Uses OT_WHOLE_LINE option type to capture complete object specifications including embedded spaces
- Template for functions includes common options like IMMUTABLE, STABLE, STRICT, SECURITY DEFINER
- Template for views provides basic SELECT structure
- Returns PSQL_CMD_NEWEDIT on successful edit to trigger execution
- Essential tool for PostgreSQL developers working with stored procedures and views
- Integrates with psql's object lookup system to resolve function/view names and retrieve definitions