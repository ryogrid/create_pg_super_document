# process_command_g_options

## Location
[src/bin/psql/command.c:1488-1562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1488-L1562)

## Overview
Processes parenthesized pset options for the \g command in psql, parsing and applying display formatting options specified within parentheses.

## Definition

```c
static backslashResult
process_command_g_options(char *first_option, PsqlScanState scan_state,
						  bool active_branch, const char *cmd)
```
## Detailed Description
This function handles the parsing and application of pset (print settings) options enclosed in parentheses that follow \g commands in psql. It iterates through options separated by spaces or commas, supporting both "name" and "name=value" formats. The function temporarily modifies print settings and can restore them if parsing fails. Options are applied only when active_branch is true, allowing for conditional execution in psql scripts.

## Parameters / Member Variables
- `*first_option`: The first option string (may be modified but not freed by this function)
- `scan_state`: Scanner state for reading additional options from input stream
- `active_branch`: Whether to actually apply the options (true) or just parse them (false)
- `*cmd`: The command name for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [savePsetInfo](../s/savePsetInfo.md)
  - do_pset
  - [restorePsetInfo](../r/restorePsetInfo.md)
  - pg_log_error
- Called from (representative examples):
  - [exec_command_g](../e/exec_command_g.md)

## Notes and Other Information
- Saves current pset state before applying options to enable rollback on failure
- Options can be restored if parsing fails after some options have been applied
- The function handles memory management carefully, never freeing the first_option parameter
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure

## Simplified Source

```c
static backslashResult
process_command_g_options(char *first_option, PsqlScanState scan_state,
                         bool active_branch, const char *cmd)
{
    bool success = true;
    bool found_r_paren = false;

    do {
        char *option;
        size_t optlen;

        // Get option (use first_option on first iteration, then scan for more)
        if (first_option) {
            option = first_option;
        } else {
            option = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
            if (!option) {
                if (active_branch) {
                    pg_log_error("\\%s: missing right parenthesis", cmd);
                    success = false;
                }
                break;
            }
        }

        // Check for terminating ')' and remove it
        optlen = strlen(option);
        if (optlen > 0 && option[optlen - 1] == ')') {
            option[--optlen] = '\0';
            found_r_paren = true;
        }

        // Process option if not empty
        if (optlen > 0) {
            // Parse "name" or "name=value" format
            char *valptr = strchr(option, '=');
            if (valptr) {
                *valptr++ = '\0';  // Split at '=' sign
            }

            if (active_branch) {
                // Save current settings on first option
                if (pset.gsavepopt == NULL) {
                    pset.gsavepopt = savePsetInfo(&pset.popt);
                }
                // Apply the pset option
                success &= do_pset(option, valptr, &pset.popt, true);
            }
        }

        // Memory management
        if (first_option) {
            first_option = NULL;  // Don't free first_option (caller owns it)
        } else {
            free(option);
        }
    } while (!found_r_paren);

    // Rollback on failure
    if (!success && active_branch && pset.gsavepopt) {
        restorePsetInfo(&pset.popt, pset.gsavepopt);
        pset.gsavepopt = NULL;
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```