# exec_query

## Location
src/bin/psql/tab-complete.c: 6213 - 6252

## Overview
Executes a SQL query safely in psql's tab completion system, returning NULL on any error to avoid interrupting the user's typing experience.

## Definition
```c
static PGresult *
exec_query(const char *query)
```

## Detailed Description
The `exec_query` function serves as the preferred way for tab completion code to communicate with the database. It provides a safe wrapper around PostgreSQL's PQexec function with built-in error handling specifically designed for tab completion scenarios. The function performs several safety checks:

1. **Input Validation**: Ensures the query string is not NULL
2. **Connection Verification**: Checks that a database connection exists and is in CONNECTION_OK state
3. **Query Execution**: Uses PQexec to execute the SQL query
4. **Result Validation**: Verifies the result status is PGRES_TUPLES_OK (successful SELECT)

Unlike normal query execution, this function deliberately suppresses error messages to avoid disrupting the user's typing experience. Errors are silently handled by returning NULL, though debugging code (conditionally compiled with NOT_USED) exists to log errors for development purposes.

## Parameters / Member Variables
- `query`: The SQL query string to execute

## Dependencies
- Functions called/Symbols referenced:
  - PQstatus (to check database connection status)
  - CONNECTION_OK (constant for valid connection state)
  - PQexec (PostgreSQL function to execute queries)
  - PGRES_TUPLES_OK (constant for successful SELECT result)
  - PQclear (to clean up failed results)
- Called from (representative examples):
  - THING_NO_SHOW (completion handling)
  - _complete_from_query (for query-based tab completion)
  - get_guctype (to retrieve GUC parameter types)

## Notes and Other Information
- Returns PGresult pointer on success, NULL on any error
- Part of psql's tab completion system in PostgreSQL
- Located in src/bin/psql/tab-complete.c at lines 6213-6252
- The function is static, meaning it's only accessible within the tab-complete.c file
- Intentionally suppresses error messages to avoid interrupting user input
- Uses global pset.db connection for database access
- Includes debugging code (conditionally compiled with NOT_USED) for development
- Caller is responsible for calling PQclear on successful results