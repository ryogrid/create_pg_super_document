# echo_hidden_command

## Location
[src/bin/psql/command.c:5574-5605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5574-L5605)

## Overview
Manages the display of hidden SQL queries according to psql's ECHO_HIDDEN setting, optionally preventing execution when set to NOEXEC mode.

## Definition

```c
static bool
echo_hidden_command(const char *query)
```
## Detailed Description
The  function implements psql's ECHO_HIDDEN functionality, which controls the visibility of internally generated SQL queries that are normally hidden from users. When ECHO_HIDDEN is enabled, these queries are displayed with decorative borders to distinguish them from user-entered commands.

The function checks the current echo_hidden setting and conditionally displays the query to both stdout and the log file (if logging is enabled). It formats the output with a distinctive comment-style border to make hidden queries easily identifiable. When the setting is PSQL_ECHO_HIDDEN_NOEXEC, it displays the query but returns false to prevent its execution, allowing users to see what would be executed without actually running it.

## Parameters / Member Variables
- `*query`: The SQL query string to potentially echo and execute
## Dependencies
- Functions called/Symbols referenced:
  - PSQL_ECHO_HIDDEN_OFF (constant indicating echo hidden is disabled)
  - PSQL_ECHO_HIDDEN_NOEXEC (constant indicating echo but don't execute mode)
- Called from (representative examples):
  - [lookup_object_oid](../l/lookup_object_oid.md) (uses this to echo object lookup queries)
  - [get_create_object_cmd](../g/get_create_object_cmd.md) (uses this to echo DDL extraction queries)

## Notes and Other Information
- Part of psql's debugging and transparency infrastructure for internal query visibility
- Outputs queries with distinctive /******* QUERY *******/ formatting for easy identification
- Supports dual output to both console and log file when logging is enabled
- Returns false when PSQL_ECHO_HIDDEN_NOEXEC is set to prevent query execution while still showing the query
- Borrowed from PSQLexec() functionality to provide consistent hidden query handling
- Essential for debugging psql's internal operations and understanding what queries are executed behind the scenes