# exec_command_ef_ev

## Location
[src/bin/psql/command.c:1177-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1177-L1292)

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
  - [strip_lineno_from_objdesc](../s/strip_lineno_from_objdesc.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md), appendPQExpBufferStr
  - [lookup_object_oid](../l/lookup_object_oid.md)
  - [get_create_object_cmd](../g/get_create_object_cmd.md)
  - [do_edit](../d/do_edit.md)
  - [ignore_slash_whole_line](../i/ignore_slash_whole_line.md)
  - pg_log_error, strncmp, strchr
- Called from (representative examples):
  - [exec_command](exec_command.md) (for both \ef and \ev commands)

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

## Simplified Source

```c
static backslashResult
exec_command_ef_ev(PsqlScanState scan_state, bool active_branch,
                   PQExpBuffer query_buf, bool is_func)
{
    backslashResult status = PSQL_CMD_SKIP_LINE;

    if (active_branch)
    {
        char *obj_desc = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, true);
        int lineno = -1;

        if (!query_buf)
        {
            pg_log_error("no query buffer");
            return PSQL_CMD_ERROR;
        }

        Oid obj_oid = InvalidOid;
        EditableObjectType eot = is_func ? EditableFunction : EditableView;

        // Extract line number from object description
        lineno = strip_lineno_from_objdesc(obj_desc);
        if (lineno == 0)
        {
            status = PSQL_CMD_ERROR;
        }
        else if (!obj_desc)
        {
            // Create template for new function/view
            resetPQExpBuffer(query_buf);
            if (is_func)
                appendPQExpBufferStr(query_buf,
                    "CREATE FUNCTION ( )\n"
                    " RETURNS \n"
                    " LANGUAGE \n"
                    " -- common options:  IMMUTABLE  STABLE  STRICT  SECURITY DEFINER\n"
                    "AS $function$\n"
                    "\n$function$\n");
            else
                appendPQExpBufferStr(query_buf,
                    "CREATE VIEW  AS\n"
                    " SELECT \n"
                    "  -- something...\n");
        }
        else if (!lookup_object_oid(eot, obj_desc, &obj_oid))
        {
            status = PSQL_CMD_ERROR;
        }
        else if (!get_create_object_cmd(eot, obj_oid, query_buf))
        {
            status = PSQL_CMD_ERROR;
        }
        else if (is_func && lineno > 0)
        {
            // Adjust line number to point to function body
            const char *lines = query_buf->data;
            while (*lines != '\0')
            {
                if (strncmp(lines, "AS ", 3) == 0 ||
                    strncmp(lines, "BEGIN ", 6) == 0 ||
                    strncmp(lines, "RETURN ", 7) == 0)
                    break;
                lineno++;
                lines = strchr(lines, '\n');
                if (!lines) break;
                lines++;
            }
        }

        if (status != PSQL_CMD_ERROR)
        {
            bool edited = false;
            if (!do_edit(NULL, query_buf, lineno, true, &edited))
                status = PSQL_CMD_ERROR;
            else if (!edited)
                puts(_("No changes"));
            else
                status = PSQL_CMD_NEWEDIT;
        }

        // Clean up on error
        if (status == PSQL_CMD_ERROR)
            resetPQExpBuffer(query_buf);

        free(obj_desc);
    }
    else
    {
        ignore_slash_whole_line(scan_state);
    }

    return status;
}
```