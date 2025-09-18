# copy_previous_query

## Location
[src/bin/psql/command.c:3317-3334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3317-L3334)

## Overview
Copies the previous query into the current query buffer if the current buffer is empty, supporting re-execution of commands.

## Definition
```c
static bool copy_previous_query(PQExpBuffer query_buf, PQExpBuffer previous_buf)
```

## Detailed Description
This utility function provides a mechanism for psql slash commands to reuse the previously executed query when no new query text has been provided. It only performs the copy operation if the current query buffer is empty (length 0), ensuring that existing query text is not overwritten. This functionality is commonly used by various slash commands where re-execution of the previous query is a typical use case.

## Parameters / Member Variables
- `query_buf`: The current query buffer to potentially copy into (can be NULL)
- `previous_buf`: The buffer containing the previous query text to copy from

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (libpq function)
- Called from (representative examples):
  - [exec_command](../e/exec_command.md)
  - [exec_command_edit](../e/exec_command_edit.md)
  - [exec_command_watch](../e/exec_command_watch.md)

## Notes and Other Information
- This is a static function used internally within psql's command processing
- Returns true if the copy operation was performed, false otherwise
- Safely handles NULL query_buf by doing nothing and returning false
- Only copies when the current query buffer is completely empty (length == 0)
- Used by slash commands like \e (edit), \watch, and others where reusing previous queries is common
- Part of psql's user convenience features for command re-execution