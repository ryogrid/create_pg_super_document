# PQntuples

## Location
src/interfaces/libpq/fe-exec.c: 3481 - 3488

## Overview
PQntuples returns the number of rows (tuples) in a query result set stored in a PGresult object.

## Definition
int PQntuples(const PGresult *res)

## Detailed Description
This function provides access to the count of rows returned by a SQL query. It is one of the fundamental functions for examining query results in the libpq interface. The function simply returns the value stored in the ntups field of the PGresult structure, which contains the number of tuples (rows) in the result set. This count is essential for applications that need to iterate through query results or determine if a query returned any data.

The function handles NULL input gracefully by returning 0, making it safe to use without explicit NULL checking in many contexts.

## Parameters / Member Variables
- : Pointer to the PGresult object containing the query results

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses res->ntups directly)
- Called from (representative examples):
  - Various PostgreSQL client applications and tools
  - Database result processing loops
  - Query validation logic

## Notes and Other Information
- Returns 0 if the PGresult pointer is NULL
- Returns the exact number of rows in the result set
- Commonly used in conjunction with PQnfields to determine result set dimensions
- Essential for iterating through query results with loops
- The returned count includes all rows, regardless of data types or NULL values
- Part of the core libpq result inspection API
- Often the first function called when examining query results
- Used by virtually all PostgreSQL client applications that process query results