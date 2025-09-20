# pg_collation_for

## Location
[src/backend/utils/adt/misc.c:619-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L619-L647)

## Overview
pg_collation_for is a SQL-callable function that implements the COLLATION FOR expression, returning the name of the collation associated with its argument.

## Definition

```c
Datum
pg_collation_for(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides SQL access to determine the collation of a given expression or column. It extracts the type information from the function's argument, validates that the type supports collations, retrieves the collation OID from the current execution context, and returns the human-readable collation name.

The function performs several validation steps: it ensures the argument type is known, verifies that the type supports collations (with a special case for UNKNOWN type), and checks that a collation is actually assigned to the expression. If any of these conditions fail, it either returns NULL or raises an appropriate error.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing the expression whose collation is to be determined

## Dependencies
- Functions called/Symbols referenced:
  -  (to extract the argument's data type)
  -  (to check if the type supports collations)
  -  (to get the collation OID from execution context)
  -  (to convert collation OID to name)
  -  (to convert C string to PostgreSQL text)
  -  (to return NULL when appropriate)
  -  (to return the result as text)
  -  (for error reporting)
  -  (for formatting type names in error messages)
  -  (constant for unknown type)
- Called from:
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- Located in src/backend/utils/adt/misc.c:619-647
- This function implements the SQL standard COLLATION FOR expression
- Returns NULL if no collation is assigned to the argument or if the argument type is unknown
- Raises an error if the argument type does not support collations (except for UNKNOWN type)
- The function works by examining the execution context rather than parsing the argument value itself
- Used primarily for debugging and introspection of collation assignments in SQL expressions