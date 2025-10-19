# exec_command_slash_command_help

## Location
[src/bin/psql/command.c:3072-3102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3072-L3102)

## Overview
Provides help functionality for psql meta-commands through the `\?` command, displaying usage information for commands, options, or variables based on the specified parameter.

## Definition
```c
static backslashResult exec_command_slash_command_help(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
This function implements the `\?` psql meta-command which provides context-sensitive help for different aspects of psql. It supports three help categories: "commands" (default), "options", and "variables". When no parameter is specified or "commands" is specified, it displays help for backslash commands via `slashUsage()`. The "options" parameter shows command-line options via `usage()`, while "variables" displays information about psql variables via `helpVariables()`. All help output respects the current pager settings.

The function follows the conditional execution pattern, only processing when `active_branch` is true, otherwise ignoring the slash options without processing.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing the help category parameter from the input
- `active_branch`: Boolean flag determining whether to execute the help command or skip processing

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option (to extract the help category parameter)
  - [slashUsage](../s/slashUsage.md) (to display backslash command help)
  - [usage](../u/usage.md) (to display command-line option help)
  - [helpVariables](../h/helpVariables.md) (to display psql variable help)
  - [ignore_slash_options](../i/ignore_slash_options.md) (to skip processing when not in active branch)
  - strcmp (for string comparison of help categories)
  - free (for memory cleanup)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher for psql meta-commands)

## Notes and Other Information
- Always returns PSQL_CMD_SKIP_LINE regardless of execution success
- Defaults to showing command help if no parameter or unrecognized parameter is provided
- Uses pset.popt.topt.pager to determine whether output should be paged
- Supports three help categories: "commands" (backslash commands), "options" (command-line options), and "variables" (psql variables)
- Part of the psql interactive help system, providing comprehensive documentation for users

## Simplified Source

```c
// Simplified version of exec_command_slash_command_help
static backslashResult exec_command_slash_command_help(PsqlScanState scan_state, bool active_branch) {
    if (active_branch) {
        // Get help category parameter
        char *opt0 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        // Display appropriate help based on category
        if (!opt0 || strcmp(opt0, "commands") == 0) {
            // Default: show backslash command help
            slashUsage(pset.popt.topt.pager);
        } else if (strcmp(opt0, "options") == 0) {
            // Show command-line options help
            usage(pset.popt.topt.pager);
        } else if (strcmp(opt0, "variables") == 0) {
            // Show psql variables help
            helpVariables(pset.popt.topt.pager);
        } else {
            // Unrecognized parameter: default to command help
            slashUsage(pset.popt.topt.pager);
        }

        free(opt0);
    } else {
        ignore_slash_options(scan_state);
    }

    return PSQL_CMD_SKIP_LINE;
}
```