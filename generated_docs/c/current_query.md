# current_query

## Location
[src/backend/utils/adt/misc.c:212-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L212-L223)

## Overview
A PostgreSQL built-in function that returns the text of the currently executing SQL query, primarily useful for debugging and logging purposes in stored procedures and functions.

## Definition

```c
struct dirent *de;
```
## Detailed Description
The current_query function provides access to the currently executing SQL statement text. It leverages the global debug_query_string variable, which contains the query text when available. This function is particularly valuable in stored procedures, triggers, and functions where you need to access information about the calling query context.

The function returns the complete query text as a PostgreSQL text type. If no query string is available (when debug_query_string is NULL), the function returns NULL. The implementation includes a comment suggesting that ActivePortal->sourceText might be used in future versions for more accurate query text retrieval.

This function is commonly used for logging, debugging, audit trails, and dynamic SQL generation where the current query context is needed.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - debug_query_string (global variable)
  - [cstring_to_text](cstring_to_text.md)
  - PG_RETURN_TEXT_P
  - PG_RETURN_NULL
- Called from (representative examples):
  - SQL queries and user-defined functions
  - No direct C code references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's standard SQL function library
- Returns NULL when no query string is available (debug_query_string is NULL)
- The debug_query_string variable is a global that may not always be populated depending on the execution context
- Useful for debugging and logging in stored procedures and triggers
- The comment suggests future enhancement using ActivePortal->sourceText for potentially more accurate query retrieval
- Returns the query as PostgreSQL's text type, which can handle arbitrary length strings
- The function is session-specific and returns the query text for the current backend process