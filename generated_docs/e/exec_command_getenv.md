# exec_command_getenv

## Location
[src/bin/psql/command.c:1580-1616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1580-L1616)

## Overview
Implements the \getenv command in psql, which retrieves a value from an environment variable and stores it in a psql variable.

## Definition

```c
static backslashResult
exec_command_getenv(PsqlScanState scan_state, bool active_branch,
					const char *cmd)
```
## Detailed Description
This function handles the \getenv backslash command which takes two arguments: a psql variable name and an environment variable name. It reads the value from the specified environment variable and assigns it to the psql variable. If the environment variable doesn't exist, the psql variable is not set. The function performs argument validation and provides error messages for missing required arguments.

## Parameters / Member Variables
- `scan_state`: Scanner state for reading command arguments from input stream
- `active_branch`: Whether to actually execute the command (true) or just parse arguments (false)
- `*cmd`: The command name for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - getenv
  - [SetVariable](../S/SetVariable.md)
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - pg_log_error
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Requires exactly two arguments: psql variable name and environment variable name
- Uses getenv() to read from the system environment
- Only sets the psql variable if the environment variable exists and has a value
- When not in active_branch, uses ignore_slash_options to skip argument parsing
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure

## Simplified Source

```c
static backslashResult exec_command_getenv(PsqlScanState scan_state, bool active_branch, const char *cmd) {
    bool success = true;

    if (active_branch) {
        // Parse two required arguments: psql var name and env var name
        char *myvar = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
        char *envvar = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (!myvar || !envvar) {
            // Missing required arguments
            pg_log_error("\\%s: missing required argument", cmd);
            success = false;
        } else {
            // Get environment variable value and set psql variable
            char *envval = getenv(envvar);
            if (envval && !SetVariable(pset.vars, myvar, envval)) {
                success = false;
            }
        }

        // Cleanup allocated strings
        free(myvar);
        free(envvar);
    } else {
        // Not in active branch - just consume arguments
        ignore_slash_options(scan_state);
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```