# complete_from_versioned_schema_query

## Location
[src/bin/psql/tab-complete.c:5192-5249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5192-L5249)

## Overview
Provides version-aware tab completion functionality for PostgreSQL schema queries by selecting an appropriate schema query based on the current server version.

## Definition
```c
static char *complete_from_versioned_schema_query(const char *text, int state)
```

## Detailed Description
This function combines the version-awareness of complete_from_versioned_query with the schema-specific functionality of complete_from_schema_query. It traverses through a SchemaQuery array (stored in completion_squery) to find the first schema query that is compatible with the current PostgreSQL server version (pset.sversion). The function checks for compatibility by examining the min_server_version field and stops when it finds a suitable query or reaches the end of the array (indicated by catname being NULL). Once found, it delegates the completion work to _complete_from_query with the selected schema query.

## Parameters / Member Variables
- `text`: The partial text that the user has typed, which needs to be completed
- `state`: The completion state used by readline for generating multiple matches

## Dependencies
- Functions called/Symbols referenced:
  - [_complete_from_query](_complete_from_query.md)
  - [SchemaQuery](../S/SchemaQuery.md) (struct type)
- Called from (representative examples):
  - COMPLETE_WITH_VERSIONED_SCHEMA_QUERY_LIST
  - THING_NO_SHOW

## Notes and Other Information
- Returns NULL if the server version is too old to support any of the available schema queries
- Relies on global variables: completion_squery, completion_charpp, completion_verbatim, and pset.sversion
- Uses catname field as a sentinel value to detect the end of the SchemaQuery array
- Part of psql's sophisticated version-aware tab completion system for database schema objects
- The function is static, indicating it's only used within the tab-complete.c file
- Less frequently used compared to complete_from_schema_query but important for version-specific schema completions