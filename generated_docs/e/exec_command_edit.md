# exec_command_edit

## Location
[src/bin/psql/command.c:1081-1176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1081-L1176)

## Overview
Implements the \e and \edit commands in psql for editing the current query buffer or a specified file using an external editor, then loading the result back into the query buffer.

## Definition
```c
static backslashResult exec_command_edit(PsqlScanState scan_state, bool active_branch, PQExpBuffer query_buf, PQExpBuffer previous_buf)
```

## Detailed Description
This function handles the \e (\edit) command which allows users to edit SQL queries using their preferred external editor. It supports multiple usage patterns: editing the current query buffer, editing a specific file and loading it into the buffer, or editing with a specific line number for cursor positioning. The function intelligently handles argument parsing where a single numeric argument is treated as a line number rather than a filename. It integrates with the psql query buffer system and handles various error conditions gracefully.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing optional filename and line number arguments
- `active_branch`: Boolean indicating whether this command should be executed or ignored due to conditional logic
- `query_buf`: PQExpBuffer containing the current query buffer that will be edited
- `previous_buf`: PQExpBuffer containing the previous query for recall if current buffer is empty

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [expand_tilde](expand_tilde.md)
  - [canonicalize_path_enc](../c/canonicalize_path_enc.md)
  - [copy_previous_query](../c/copy_previous_query.md)
  - [do_edit](../d/do_edit.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - pg_log_error, atoi, strspn, strlen
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher)

## Notes and Other Information
- Supports syntax: \e [filename] [line_number] or \e [line_number]
- If only a numeric argument is provided, it's treated as a line number for the current buffer
- Uses expand_tilde to handle ~ in file paths
- Calls canonicalize_path_enc to normalize file paths according to client encoding
- If query buffer is empty, recalls previous query for editing
- Returns PSQL_CMD_NEWEDIT on successful edit to trigger query buffer refresh
- Resets query buffer on error to prevent corrupted state
- Essential for interactive SQL development workflow in psql

## Simplified Source

```c
static backslashResult
exec_command_edit(PsqlScanState scan_state, bool active_branch,
                  PQExpBuffer query_buf, PQExpBuffer previous_buf)
{
    backslashResult status = PSQL_CMD_SKIP_LINE;

    if (active_branch)
    {
        if (!query_buf)
        {
            pg_log_error("no query buffer");
            return PSQL_CMD_ERROR;
        }

        // Parse optional filename and line number arguments
        char *fname = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
        char *ln = NULL;
        int lineno = -1;

        if (fname)
        {
            // Try to get separate line number argument
            ln = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
            if (ln == NULL && fname[0] && strspn(fname, "0123456789") == strlen(fname))
            {
                // Single numeric argument - treat as line number
                ln = fname;
                fname = NULL;
            }
        }

        // Process line number if provided
        if (ln)
        {
            lineno = atoi(ln);
            if (lineno < 1)
            {
                pg_log_error("invalid line number: %s", ln);
                status = PSQL_CMD_ERROR;
            }
        }

        if (status != PSQL_CMD_ERROR)
        {
            bool discard_on_quit;

            // Handle file path expansion and setup
            expand_tilde(&fname);
            if (fname)
            {
                canonicalize_path_enc(fname, pset.encoding);
                discard_on_quit = true;
            }
            else
            {
                // Use previous query if current buffer is empty
                discard_on_quit = copy_previous_query(query_buf, previous_buf);
            }

            // Launch editor and handle result
            if (do_edit(fname, query_buf, lineno, discard_on_quit, NULL))
                status = PSQL_CMD_NEWEDIT;
            else
                status = PSQL_CMD_ERROR;
        }

        // Clean up on error
        if (status == PSQL_CMD_ERROR)
            resetPQExpBuffer(query_buf);

        free(fname);
        free(ln);
    }
    else
    {
        ignore_slash_options(scan_state);
    }

    return status;
}
```