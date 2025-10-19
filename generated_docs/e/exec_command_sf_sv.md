# exec_command_sf_sv

## Location
[src/bin/psql/command.c:2522-2604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2522-L2604)

## Overview
Implements the `\sf` (show function) and `\sv` (show view) psql commands that display the source code definition of functions and views.

## Definition
```c
static backslashResult
exec_command_sf_sv(PsqlScanState scan_state, bool active_branch,
                   const char *cmd, bool is_func)
```

## Detailed Description
This function handles the `\sf` and `\sv` psql meta-commands which allow users to view the source code of database functions and views respectively. The function performs object name parsing, validates the existence of the specified object, retrieves its definition, and outputs it with optional line numbering. It supports output redirection and automatic paging for long definitions.

The function checks for the presence of a `+` modifier in the command to determine whether to show line numbers. It uses the PostgreSQL object lookup system to validate the existence of the requested function or view, then retrieves the complete CREATE statement for display.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing the command line input
- `active_branch`: Boolean indicating if the command should be executed or skipped
- `cmd`: String containing the full command text (used to check for `+` modifier)
- `is_func`: Boolean flag - true for `\sf` (function), false for `\sv` (view)

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option` - Parse object name from command line
  - [lookup_object_oid](../l/lookup_object_oid.md) - Validate and get OID of the specified object
  - [get_create_object_cmd](../g/get_create_object_cmd.md) - Retrieve the CREATE statement for the object
  - [count_lines_in_buf](../c/count_lines_in_buf.md) - Count lines in output buffer for paging decisions
  - [PageOutput](../P/PageOutput.md) - Set up paged output if needed
  - [print_with_linenumbers](../p/print_with_linenumbers.md) - Output with line number formatting
  - [ignore_slash_whole_line](../i/ignore_slash_whole_line.md) - Skip parsing when not in active branch
- Called from:
  - [exec_command](exec_command.md) - Main command dispatcher for `\sf` and `\sv` commands

## Notes and Other Information
- Supports both functions (`\sf`) and views (`\sv`) through the `is_func` parameter
- The `+` modifier enables line numbering in the output
- Automatically uses a pager for long definitions when outputting to stdout
- Respects psql's output redirection settings (`\o` command)
- Error handling includes validation of object names and existence
- Uses EditableObjectType enumeration to distinguish between functions and views
- Source code location: src/bin/psql/command.c:2522-2604

## Simplified Source

```c
static backslashResult
exec_command_sf_sv(PsqlScanState scan_state, bool active_branch,
                   const char *cmd, bool is_func)
{
    backslashResult status = PSQL_CMD_SKIP_LINE;

    if (active_branch) {
        // Check for line number option (+)
        bool show_linenumbers = (strchr(cmd, '+') != NULL);
        PQExpBuffer buf = createPQExpBuffer();
        char *obj_desc;
        Oid obj_oid = InvalidOid;
        EditableObjectType eot = is_func ? EditableFunction : EditableView;

        // Parse object name from command line
        obj_desc = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, true);

        if (!obj_desc) {
            // Report missing function/view name
            pg_log_error(is_func ? "function name is required" : "view name is required");
            status = PSQL_CMD_ERROR;
        }
        else if (!lookup_object_oid(eot, obj_desc, &obj_oid)) {
            // Object lookup failed
            status = PSQL_CMD_ERROR;
        }
        else if (!get_create_object_cmd(eot, obj_oid, buf)) {
            // Failed to get object definition
            status = PSQL_CMD_ERROR;
        }
        else {
            // Output the source code
            FILE *output;
            bool is_pager;

            // Determine output destination (pager vs direct)
            if (pset.queryFout == stdout) {
                int lineno = count_lines_in_buf(buf);
                output = PageOutput(lineno, &(pset.popt.topt));
                is_pager = true;
            } else {
                output = pset.queryFout;
                is_pager = false;
            }

            // Output with or without line numbers
            if (show_linenumbers) {
                print_with_linenumbers(output, buf->data, is_func);
            } else {
                fputs(buf->data, output);
            }

            if (is_pager)
                ClosePager(output);
        }

        free(obj_desc);
        destroyPQExpBuffer(buf);
    }
    else {
        ignore_slash_whole_line(scan_state);
    }

    return status;
}
```