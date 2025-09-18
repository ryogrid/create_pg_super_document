# exec_command_pset

## Location
[src/bin/psql/command.c:2278-2332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2278-L2332)

## Overview
Implements the PostgreSQL psql `\pset` command that configures output formatting parameters and display settings for query results.

## Definition
```c
static backslashResult exec_command_pset(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
The `exec_command_pset` function handles the `\pset` backslash command in psql, which controls various aspects of query output formatting. When called without arguments, it lists all available formatting parameters and their current values. When called with one or two arguments, it sets a specific formatting parameter. The function supports numerous formatting options including border styles, field separators, output format (table, CSV, HTML, etc.), paging behavior, and Unicode styling options. It delegates the actual parameter setting to the `do_pset` function.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line arguments and options
- `active_branch`: Boolean indicating if the command should be executed (used for conditional execution in psql scripts)

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option()` - Parses command arguments (parameter name and value)
  - [pset_value_string](../p/pset_value_string.md)() - Gets string representation of current parameter values
  - `do_pset()` - Actually sets the specified parameter to the given value
  - `printf()` - Displays parameter names and values
  - `free()` - Memory management
  - [ignore_slash_options](../i/ignore_slash_options.md)() - Handles unused options when inactive
- Called from (representative examples):
  - [exec_command](exec_command.md) - Main command dispatcher in psql

## Notes and Other Information
- Returns `PSQL_CMD_SKIP_LINE` on success, `PSQL_CMD_ERROR` on failure
- When called with no arguments, displays all 22 available formatting parameters with their current values
- Supported parameters include: border, columns, csv_fieldsep, expanded, fieldsep, fieldsep_zero, footer, format, linestyle, null, numericlocale, pager, pager_min_lines, recordsep, recordsep_zero, tableattr, title, tuples_only, unicode_border_linestyle, unicode_column_linestyle, unicode_header_linestyle, xheader_width
- Only executes when `active_branch` is true, supporting conditional execution in psql scripts
- Parameter values are stored in the global `pset.popt` structure
- Respects the quiet mode setting (`pset.quiet`) when setting parameters
- Located in `src/bin/psql/command.c:2278-2332`
- Essential for customizing psql output appearance and behavior