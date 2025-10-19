# exec_command_bind

## Location
[src/bin/psql/command.c:485-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L485-L520)

## Overview
exec_command_bind implements the \bind backslash command that sets query parameters for prepared statement execution in PostgreSQL psql.

## Definition

```c
static backslashResult
exec_command_bind(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
exec_command_bind collects parameter values from the command line arguments and stores them in the global pset structure for use with prepared statements. The function first calls clean_bind_state() to clear any existing parameter bindings, then parses all arguments as parameter values using psql_scan_slash_option(). The parameters are stored in a dynamically allocated array that grows as needed using pg_realloc_array().

When active_branch is false (inside a false \if block), the function calls ignore_slash_options() to consume and discard the arguments without processing them. After successful parameter collection, the function sets pset.bind_flag to true to indicate that parameters are available for the next query execution.

## Parameters / Member Variables
- `scan_state`: Lexer working state used to parse command line arguments
- `active_branch`: Boolean indicating whether the command should actually execute (false when inside a false \if block)

## Dependencies
- Functions called/Symbols referenced:
  - [clean_bind_state](../c/clean_bind_state.md)
  - psql_scan_slash_option
  - pg_realloc_array
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - OT_NORMAL (option type constant)
- Called from (representative examples):
  - [exec_command](exec_command.md) (src/bin/psql/command.c:331)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on successful completion
- Parameters are stored in pset.bind_params array with count in pset.bind_nparams
- Uses dynamic memory allocation that doubles the array size when more space is needed
- Sets pset.bind_flag to true to signal that bound parameters are available
- Properly handles conditional execution by ignoring arguments when not in active branch
- The bound parameters will be used by subsequent query execution that supports prepared statement parameters
- Memory management includes cleaning previous bindings before setting new ones

## Simplified Source

```c
static backslashResult exec_command_bind(PsqlScanState scan_state, bool active_branch) {
    backslashResult status = PSQL_CMD_SKIP_LINE;

    if (active_branch) {
        char *opt;
        int nparams = 0;
        int nalloc = 0;

        // Clear any existing parameter bindings
        clean_bind_state();

        // Parse all arguments as parameter values
        while ((opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false))) {
            nparams++;

            // Expand parameter array if needed
            if (nparams > nalloc) {
                nalloc = nalloc ? nalloc * 2 : 1;
                pset.bind_params = pg_realloc_array(pset.bind_params, char *, nalloc);
            }

            pset.bind_params[nparams - 1] = opt;
        }

        // Set parameter count and flag
        pset.bind_nparams = nparams;
        pset.bind_flag = true;
    } else {
        // Ignore arguments when in false \if branch
        ignore_slash_options(scan_state);
    }

    return status;
}
```