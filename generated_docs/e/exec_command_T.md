# exec_command_T

## Location
[src/bin/psql/command.c:2627-2648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2627-L2648)

## Overview
Implements the `\T` psql command that sets HTML table attributes for HTML output format.

## Definition
```c
static backslashResult
exec_command_T(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
This function handles the `\T` psql meta-command which allows users to specify HTML table attributes that will be applied to the `<table>` tag when psql output format is set to HTML. The command accepts a string parameter containing the attributes to be inserted into the HTML table tag. This enables users to control the styling and behavior of HTML table output by specifying attributes like `border`, `class`, `style`, etc.

The function uses the standard psql option parsing mechanism and delegates the actual setting to `do_pset` with the "tableattr" option name.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line options
- `active_branch`: Boolean indicating if the command should be executed or skipped

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option` - Parse attribute string from command line
  - `do_pset` - Set the tableattr print option
  - [ignore_slash_options](../i/ignore_slash_options.md) - Skip option parsing when not in active branch
- Called from:
  - [exec_command](exec_command.md) - Main command dispatcher for `\T` command

## Notes and Other Information
- Only affects output when psql is set to HTML format (`\H` command)
- The attributes string is inserted verbatim into the HTML `<table>` tag
- Common usage includes setting CSS classes, border attributes, or inline styles
- Example: `\T border="1" class="data-table"` results in `<table border="1" class="data-table">`
- Setting an empty value (`\T` with no parameter) clears any previously set attributes
- The setting persists for the duration of the psql session unless changed again
- Source code location: src/bin/psql/command.c:2627-2648

## Simplified Source

```c
static backslashResult
exec_command_T(PsqlScanState scan_state, bool active_branch)
{
    bool success = true;

    if (active_branch) {
        // Parse HTML table attributes string
        char *value = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        // Set tableattr option for HTML output format
        success = do_pset("tableattr", value, &pset.popt, pset.quiet);
        free(value);
    }
    else {
        ignore_slash_options(scan_state);
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```