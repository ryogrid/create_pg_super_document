# describeFunctions

## Location
[src/bin/psql/describe.c:288-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L288-L614)

## Overview
Implements the \df psql command to display a comprehensive list of functions in the database, supporting multiple function types, pattern matching, argument filtering, and extensive verbose information.

## Definition
```c
bool describeFunctions(const char *functypes, const char *func_pattern,
                      char **arg_patterns, int num_arg_patterns,
                      bool verbose, bool showSystem)
```

## Detailed Description
This is the most complex describe function in psql, handling the \df command with sophisticated filtering capabilities. It constructs dynamic SQL queries to list functions from pg_proc and related catalogs, supporting function type filtering (aggregate, normal, procedure, trigger, window), pattern matching on function names, argument type pattern matching, and comprehensive verbose output. The function handles PostgreSQL version differences (particularly for procedures introduced in v11 and parallel safety in v9.6) and provides extensive internationalization support.

## Parameters / Member Variables
- `functypes`: String containing function type specifiers ('a'=aggregate, 'n'=normal, 'p'=procedure, 't'=trigger, 'w'=window)
- `func_pattern`: Optional regular expression pattern to filter functions by name or schema
- `arg_patterns`: Array of patterns to match against function argument types
- `num_arg_patterns`: Number of argument patterns provided
- `verbose`: Boolean flag to include extensive additional information (volatility, parallel safety, owner, security, ACL, language, etc.)
- `showSystem`: Boolean flag to control whether system schema functions are displayed

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [map_typename_pattern](../m/map_typename_pattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_dfo](../e/exec_command_dfo.md) (in command.c:1062)

## Notes and Other Information
- Part of psql's describe functionality (\df command)
- Supports complex function type filtering with multiple simultaneous options
- Handles PostgreSQL version compatibility for procedures (v11+) and parallel safety (v9.6+)
- Uses dynamic JOIN construction for argument type pattern matching
- Provides comprehensive verbose output including volatility, parallel safety, ownership, security context, ACL, and language information
- Supports special '-' pattern in argument matching to specify 'no parameter at this position'
- Uses different column translation arrays for different PostgreSQL versions
- Includes sophisticated WHERE clause construction based on function type selections
- Validates function type string against allowed characters (anptwS+)
- Returns early with helpful error messages for unsupported options on older servers