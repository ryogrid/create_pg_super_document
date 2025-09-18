# exec_command_d

## Location
src/bin/psql/command.c: 773 - 1038

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