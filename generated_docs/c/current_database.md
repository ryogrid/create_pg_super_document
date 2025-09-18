# current_database

## Location
src/backend/utils/adt/misc.c: 195 - 211

## Overview
A PostgreSQL built-in function that returns the name of the current database that the session is connected to.

## Definition


## Detailed Description
The current_database function provides a way for SQL queries and applications to programmatically determine which database they are currently connected to. It retrieves the database name using the global MyDatabaseId variable and converts it to a user-readable string format.

The function allocates memory for a Name structure (PostgreSQL's internal string type for identifiers), retrieves the database name corresponding to the current database OID, and returns it as a SQL-accessible value. This is particularly useful in multi-database environments, logging scenarios, or when writing portable SQL code that needs to be database-aware.

The function always returns a valid database name since a PostgreSQL session must be connected to a specific database.

## Parameters / Member Variables
- Uses the standard PostgreSQL function call interface 
- Takes no actual parameters (parameterless function)

## Dependencies
- Functions called/Symbols referenced:
  - palloc (for memory allocation)
  - get_database_name
  - namestrcpy
  - PG_RETURN_NAME
  - MyDatabaseId (global variable)
  - NAMEDATALEN (constant for name length)
- Called from (representative examples):
  - ExecEvalSQLValueFunction
  - SQL queries and user-defined functions

## Notes and Other Information
- This function is part of PostgreSQL's standard SQL function library and is SQL-standard compliant
- Returns the database name as a PostgreSQL 'name' type, which has a maximum length of NAMEDATALEN characters
- The function allocates memory using palloc, which is automatically freed at the end of the current memory context
- MyDatabaseId is a global variable that contains the OID of the current database
- The function is commonly used in system catalogs, monitoring queries, and administrative scripts
- Cannot return NULL under normal circumstances since every PostgreSQL session is connected to a database