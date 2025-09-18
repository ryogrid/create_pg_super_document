# psql_add_command

## Location
[src/test/regress/pg_regress.c:1127-1163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1127-L1163)

## Overview
Adds a SQL command to a psql command string buffer with proper shell escaping and variable argument formatting, preparing it for safe execution via system().

## Definition
```c
static void psql_add_command(StringInfo buf, const char *query,...)
```

## Detailed Description
This function is the second part of the three-function psql command building suite. It takes a SQL query string with optional printf-style format arguments and safely adds it to the psql command buffer as a -c argument. The function handles two critical aspects of command construction: variable argument formatting and shell metacharacter escaping.

The function first uses a variadic argument mechanism to format the query string with any provided arguments, similar to sprintf. It then performs shell escaping by prefixing backslashes before characters that have special meaning in shell double-quote contexts (backslash, double-quote, dollar sign, and backtick). This ensures the SQL command will be properly interpreted by psql when executed via system().

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the psql command being built
- `query`: SQL query string, potentially containing printf-style format specifiers
- `...`: Variable arguments corresponding to format specifiers in the query string

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString
  - initStringInfo
  - appendStringInfoVA
  - enlargeStringInfo
  - appendStringInfoChar
  - [pfree](pfree.md)
  - strchr (standard C library)
- Called from (representative examples):
  - psql_command
  - [drop_database_if_exists](../d/drop_database_if_exists.md)
  - [create_database](../c/create_database.md)
  - [drop_role_if_exists](../d/drop_role_if_exists.md)
  - [create_role](../c/create_role.md)

## Notes and Other Information
- This function must be called after psql_start_command() and before psql_end_command()
- The function can be called multiple times to add multiple SQL commands to a single psql invocation
- Shell metacharacter escaping specifically targets characters meaningful within double quotes: backslash, double-quote, dollar sign, and backtick
- Uses a retry loop for string formatting to handle cases where the initial buffer size is insufficient
- Memory allocated for the temporary command buffer is properly freed after use
- The function wraps each SQL command in double quotes as a -c argument to psql