# getdatabaseencoding

## Location
src/backend/utils/mb/mbutils.c: 1273 - 1278

## Overview
SQL-callable function that returns the database encoding name as a PostgreSQL NAME data type, making it accessible from SQL queries and stored procedures.

## Definition
Datum getdatabaseencoding(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the SQL interface to retrieve the database encoding name. It's designed to be called from SQL using the getdatabaseencoding() function and returns the encoding name as a PostgreSQL NAME data type rather than a plain C string.

The function converts the database encoding name (stored as a C string in DatabaseEncoding->name) into a PostgreSQL Datum by using the namein input function. This transformation allows the encoding name to be properly handled within the PostgreSQL SQL execution environment, including being returned to clients, stored in variables, and used in expressions.

The function follows PostgreSQL's convention for SQL-callable functions by taking PG_FUNCTION_ARGS as a parameter (even though it doesn't use any arguments) and returning a Datum type.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1 (PostgreSQL function call mechanism)
  - namein (input function for NAME data type)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to Datum)
  - DatabaseEncoding (global structure containing encoding information)
- Called from (representative examples):
  - No direct references found (typically called through SQL function dispatch mechanism)

## Notes and Other Information
- This is the SQL-callable version of GetDatabaseEncodingName()
- Returns a NAME data type rather than a plain C string
- Part of PostgreSQL's system function catalog, callable via SQL as getdatabaseencoding()
- Despite taking PG_FUNCTION_ARGS, the function ignores any arguments since it has no parameters
- Used internally by SQL queries that need to determine the current database encoding
- The returned value can be compared, stored, or manipulated like any other NAME value in SQL