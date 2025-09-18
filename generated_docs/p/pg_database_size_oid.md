# pg_database_size_oid

## Location
[src/backend/utils/adt/dbsize.c:168-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L168-L181)

## Overview
A PostgreSQL SQL-callable function that returns the total disk space used by a database identified by its OID (Object Identifier).

## Definition


## Detailed Description
The  function serves as a PostgreSQL built-in function that can be called from SQL to get the size of a database. It extracts the database OID from the function arguments using the PostgreSQL function call interface, delegates the actual size calculation to the internal  function, and returns the result. If the calculated size is 0 (typically indicating the database doesn't exist or is inaccessible), the function returns NULL; otherwise, it returns the size as a 64-bit integer representing bytes.

## Parameters / Member Variables
- Function takes one argument via :
  - : The OID of the database whose size should be calculated (extracted via )

## Dependencies
- Functions called/Symbols referenced:
  - : Performs the actual database size calculation
  - : Macro to extract OID argument from function call
  - : Macro to return NULL value
  - : Macro to return 64-bit integer value
- Called from (representative examples):
  - SQL queries using the pg_database_size(oid) function
  - System catalog queries and administrative scripts

## Notes and Other Information
- This is a public PostgreSQL built-in function accessible from SQL
- Returns NULL when database size is 0, which typically means the database doesn't exist or user lacks privileges
- The function signature follows PostgreSQL's internal function calling convention
- Size is returned in bytes as a 64-bit integer to handle very large databases
- This function is typically exposed to SQL as pg_database_size(oid) and may also be used by higher-level functions
- Access control is handled by the underlying calculate_database_size function