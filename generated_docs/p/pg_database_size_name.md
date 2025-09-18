# pg_database_size_name

## Location
src/backend/utils/adt/dbsize.c: 182 - 201

## Overview
A PostgreSQL SQL-callable function that returns the total disk space used by a database identified by its name.

## Definition


## Detailed Description
The  function serves as a PostgreSQL built-in function that can be called from SQL to get the size of a database using its name rather than OID. It extracts the database name from the function arguments, converts it to an OID using , then delegates to  for the actual size calculation. The function will raise an error if the database name doesn't exist (due to the  parameter passed to ). Like its OID counterpart, it returns NULL if the calculated size is 0, otherwise returns the size as a 64-bit integer in bytes.

## Parameters / Member Variables
- Function takes one argument via :
  - : The name of the database whose size should be calculated (extracted via )

## Dependencies
- Functions called/Symbols referenced:
  - : Converts database name to OID, with error on non-existence
  - : Performs the actual database size calculation
  - : Macro to extract Name argument from function call
  - : Macro to extract string from Name data type
  - : Macro to return NULL value
  - : Macro to return 64-bit integer value
  - : PostgreSQL data type for names
- Called from (representative examples):
  - SQL queries using the pg_database_size(name) function
  - System catalog queries and administrative scripts

## Notes and Other Information
- This is a public PostgreSQL built-in function accessible from SQL
- More user-friendly than the OID version as users typically know database names rather than OIDs
- Will raise an error if the database name doesn't exist (unlike some variants that might return NULL)
- Returns NULL when database size is 0, which typically means the user lacks privileges to access the database
- The function signature follows PostgreSQL's internal function calling convention
- Size is returned in bytes as a 64-bit integer to handle very large databases
- This function is typically exposed to SQL as pg_database_size(text) and provides a name-based interface to database size calculation
- Access control is handled by the underlying calculate_database_size function