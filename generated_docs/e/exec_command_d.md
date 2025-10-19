# exec_command_d

## Location
[src/bin/psql/command.c:773-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L773-L1038)

## Overview
Implements the comprehensive \d family of commands in psql for describing and listing database objects, providing detailed information about tables, functions, types, and other PostgreSQL objects.

## Definition
```c
static backslashResult exec_command_d(PsqlScanState scan_state, bool active_branch, const char *cmd)
```

## Detailed Description
This function serves as the central dispatcher for all \d commands in psql, which are used to describe and list database objects. It parses the command variant (e.g., \dt for tables, \df for functions, \du for users) and extracts modifiers like '+' for verbose output and 'S' for system objects. The function then delegates to appropriate specialized functions based on the command pattern. It supports pattern matching for filtering results and handles conditional execution within psql scripts.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command arguments and patterns
- `active_branch`: Boolean indicating whether this command should be executed or ignored due to conditional logic
- `cmd`: String containing the full command (e.g., "dt+", "df", "du") to determine which description function to call

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [describeTableDetails](../d/describeTableDetails.md), listTables, describeAggregates, describeTablespaces
  - [listConversions](../l/listConversions.md), listCasts, objectDescription, listDomains
  - [exec_command_dfo](exec_command_dfo.md) (for functions and operators)
  - [describeRoles](../d/describeRoles.md), listLargeObjects, listLanguages, listSchemas
  - [permissionsList](../p/permissionsList.md), listPartitionedTables, describeTypes
  - [listTSConfigs](../l/listTSConfigs.md), listTSParsers, listTSDictionaries, listTSTemplates
  - [listForeignServers](../l/listForeignServers.md), listUserMappings, listForeignDataWrappers
  - [listExtensions](../l/listExtensions.md), listExtendedStats, listEventTriggers
  - [ignore_slash_options](../i/ignore_slash_options.md)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher)

## Notes and Other Information
- Supports numerous command variants: \d, \dt, \di, \ds, \dv, \dm, \dE, \df, \da, \db, \dc, \dC, \dd, \dD, \dg, \dl, \dL, \dn, \do, \dO, \dp, \dP, \dT, \dr, \dR, \du, \dF, \de, \dx, \dX, \dy
- Modifiers: '+' for verbose output, 'S' to include system objects
- Each command variant calls specialized listing/description functions from describe.c
- Handles complex commands like \dA (access methods), \dF (text search), \de (foreign data wrappers)
- Returns PSQL_CMD_UNKNOWN for unrecognized command variants
- Essential part of psql's introspection capabilities for database exploration

## Simplified Source

```c
static backslashResult
exec_command_d(PsqlScanState scan_state, bool active_branch, const char *cmd)
{
    backslashResult status = PSQL_CMD_SKIP_LINE;
    bool success = true;

    if (active_branch) {
        // Parse optional pattern for filtering results
        char *pattern = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        // Extract modifiers from command
        bool show_verbose = strchr(cmd, '+') ? true : false;
        bool show_system = strchr(cmd, 'S') ? true : false;

        // Dispatch based on command variant
        switch (cmd[1]) {
            case '\0': case '+': case 'S':
                // \d - describe tables or list all interesting objects
                success = pattern ? describeTableDetails(pattern, show_verbose, show_system)
                                  : listTables("tvmsE", NULL, show_verbose, show_system);
                break;

            case 'A':
                // \dA - access methods and related objects
                success = handle_access_methods(scan_state, cmd, pattern, show_verbose);
                break;

            case 'a': success = describeAggregates(pattern, show_verbose, show_system); break;
            case 'b': success = describeTablespaces(pattern, show_verbose); break;
            case 'c': success = handle_conversions_or_config(cmd, pattern, show_verbose, show_system); break;
            case 'C': success = listCasts(pattern, show_verbose); break;
            case 'd': success = handle_descriptions_or_defaults(cmd, pattern, show_system); break;
            case 'D': success = listDomains(pattern, show_verbose, show_system); break;

            case 'f': case 'o':
                // \df, \do - functions and operators (delegated to exec_command_dfo)
                success = exec_command_dfo(scan_state, cmd, pattern, show_verbose, show_system);
                break;

            case 'g': case 'u': success = describeRoles(pattern, show_verbose, show_system); break;
            case 'l': success = listLargeObjects(show_verbose); break;
            case 'L': success = listLanguages(pattern, show_verbose, show_system); break;
            case 'n': success = listSchemas(pattern, show_verbose, show_system); break;
            case 'O': success = listCollations(pattern, show_verbose, show_system); break;
            case 'p': success = permissionsList(pattern, show_system); break;

            case 'P':
                // \dP - partitioned tables
                success = listPartitionedTables(&cmd[2], pattern, show_verbose);
                break;

            case 'T': success = describeTypes(pattern, show_verbose, show_system); break;

            case 't': case 'v': case 'm': case 'i': case 's': case 'E':
                // Various table types
                success = listTables(&cmd[1], pattern, show_verbose, show_system);
                break;

            case 'r': success = handle_role_settings_or_grants(scan_state, cmd, pattern, show_system); break;
            case 'R': success = handle_publications_or_subscriptions(cmd, pattern, show_verbose); break;
            case 'F': success = handle_text_search(cmd, pattern, show_verbose); break;
            case 'e': success = handle_foreign_data(cmd, pattern, show_verbose); break;
            case 'x': success = show_verbose ? listExtensionContents(pattern) : listExtensions(pattern); break;
            case 'X': success = listExtendedStats(pattern); break;
            case 'y': success = listEventTriggers(pattern, show_verbose); break;

            default:
                status = PSQL_CMD_UNKNOWN;
        }

        free(pattern);
    } else {
        ignore_slash_options(scan_state);
    }

    return success ? status : PSQL_CMD_ERROR;
}
```