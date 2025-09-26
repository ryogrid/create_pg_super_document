# describeOperators

## Location
[src/bin/psql/describe.c:770-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L770-L910)

## Overview
Implements the \do psql command to describe PostgreSQL operators, displaying their names, argument types, result types, and optionally their underlying functions and descriptions.

## Definition

```c
bool
describeOperators(const char *oper_pattern,
				  char **arg_patterns, int num_arg_patterns,
				  bool verbose, bool showSystem)
```
## Detailed Description
This function generates and executes a complex SQL query to retrieve operator information from PostgreSQL system catalogs. It constructs a detailed view of operators including their schema, name, left/right argument types, result type, and descriptions. The function supports pattern matching for both operator names and argument types, with special handling for prefix operators (when only one argument pattern is provided).

The function builds a SQL query that joins pg_operator with pg_namespace and optionally with pg_type tables for argument type filtering. It includes backward compatibility support for postfix operators (dead code as of PostgreSQL 14 but maintained for older server versions) and provides fallback comment lookup from the operator's underlying function for operators without direct comments.

The query results are formatted and displayed using psql's standard table printing mechanisms with appropriate column headers and internationalization support.

## Parameters / Member Variables
- : Pattern to match operator names (can be NULL for all operators)
- : Array of patterns to match argument types (can contain "-" for no argument)
- : Number of argument patterns provided (0-2, additional patterns ignored)
- : If true, includes the underlying function name in output
- : If true, includes system schema operators (pg_catalog, information_schema)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize query buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format SQL query)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and add pattern matching clauses)
  - [map_typename_pattern](../m/map_typename_pattern.md) (normalize type name patterns)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - [printQuery](../p/printQuery.md) (display results)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup query buffer)
- Called from (representative examples):
  - [exec_command_dfo](../e/exec_command_dfo.md) (src/bin/psql/command.c:1066) - handles \do command
  - Declared in DESCRIBE_H (src/bin/psql/describe.h:30)

## Notes and Other Information
- Supports complex operator filtering by both name and argument type patterns
- Handles unary prefix operators when num_arg_patterns == 1 by adding "o.oprleft = 0" constraint
- Uses coalesce() to provide fallback comment lookup from operator's function (legacy support)
- Maintains compatibility with pre-PostgreSQL 14 servers by including postfix operator support
- Argument patterns of "-" specifically indicate no argument should exist for that position
- Results are ordered by schema, name, left arg type, and right arg type for consistent display
- Returns boolean success/failure status like other psql describe functions
- The verbose flag adds the "Function" column showing the underlying implementation function