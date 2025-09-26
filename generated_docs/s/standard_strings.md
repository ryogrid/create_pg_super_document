# standard_strings

## Location
[src/bin/psql/common.c:2132-2151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L2132-L2151)

## Overview
This function checks whether the current PostgreSQL session is using standard-conforming string literals by querying the server's parameter status.

## Definition

```c
bool
standard_strings(void)
```
## Detailed Description
The  function is a utility function in psql that determines if the current database session has the  parameter enabled. This setting controls how PostgreSQL interprets backslash escapes in string literals. When enabled ("on"), backslashes in string literals are treated literally rather than as escape characters, conforming to the SQL standard.

The function first checks if there's an active database connection, returning  if no connection exists. If connected, it queries the server's  parameter using  and returns  if the value is "on",  otherwise.

This information is important for psql's string processing and command parsing, as it affects how escape sequences should be interpreted in user input and SQL commands.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [PQparameterStatus](../P/PQparameterStatus.md) (libpq function to query server parameters)
  - pset.db (global psql database connection)
- Called from (representative examples):
  - [get_create_object_cmd](../g/get_create_object_cmd.md) (for generating SQL commands)
  - [parse_slash_copy](../p/parse_slash_copy.md) (for COPY command parsing)
  - [MainLoop](../M/MainLoop.md) (in psql main loop for input processing)

## Notes and Other Information
- This function is specific to psql client application, not the PostgreSQL backend
- The  setting affects how backslash escapes are interpreted in string literals
- When standard_conforming_strings is off, backslashes act as escape characters (legacy PostgreSQL behavior)
- When on, backslashes are treated literally, conforming to SQL standard behavior
- This setting is crucial for proper handling of file paths, regular expressions, and other strings containing backslashes
- The function helps psql adapt its string processing behavior based on the server's configuration