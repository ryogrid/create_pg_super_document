# complete_from_schema_query

## Location
[src/bin/psql/tab-complete.c:5184-5191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5184-L5191)

## Overview
Provides tab completion functionality for PostgreSQL schema-related commands using a schema query that works across all server versions.

## Definition
```c
static char *complete_from_schema_query(const char *text, int state)
```

## Detailed Description
This function implements schema-aware tab completion in psql by using a predefined schema query stored in the global variable completion_squery. Unlike complete_from_versioned_query, this function assumes the query is compatible with any PostgreSQL server version and directly delegates the completion work to _complete_from_query. This is typically used for completing schema names, table names, column names, and other database object names that have consistent query patterns across PostgreSQL versions.

## Parameters / Member Variables
- `text`: The partial text that the user has typed, which needs to be completed
- `state`: The completion state used by readline for generating multiple matches

## Dependencies
- Functions called/Symbols referenced:
  - [_complete_from_query](_complete_from_query.md)
- Called from (representative examples):
  - COMPLETE_WITH_SCHEMA_QUERY_LIST
  - COMPLETE_WITH_SCHEMA_QUERY_VERBATIM
  - COMPLETE_WITH_ATTR_LIST
  - COMPLETE_WITH_ENUM_VALUE
  - COMPLETE_WITH_FUNCTION_ARG
  - THING_NO_SHOW

## Notes and Other Information
- The query is assumed to work for any PostgreSQL server version, eliminating the need for version checking
- Relies on global variables: completion_squery, completion_charpp, and completion_verbatim
- Part of psql's comprehensive tab completion system for database objects
- The function is static, indicating it's only used within the tab-complete.c file
- More widely used than complete_from_versioned_query, as evidenced by the number of callers

## Simplified Source

```c
static char *complete_from_schema_query(const char *text, int state)
{
    // Simple wrapper for schema-aware completions
    // Assumes query works for any server version
    return _complete_from_query(NULL,                    // No regular query
                               completion_squery,        // Schema query structure
                               completion_charpp,        // Result processing
                               completion_verbatim,      // Verbatim matching flag
                               text,                     // User's partial input
                               state);                   // Readline state counter
}
```