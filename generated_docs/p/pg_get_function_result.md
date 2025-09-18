# pg_get_function_result

## Location
src/backend/utils/adt/ruleutils.c: 3184 - 3213

## Overview
Returns a nicely-formatted version of a function's return type, representing what would appear after the RETURNS clause in a CREATE FUNCTION statement.

## Definition
```c
Datum pg_get_function_result(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function takes a function OID as input and returns a formatted text representation of the function's return type. It retrieves the function's metadata from the pg_proc system catalog and formats the return type specification in a human-readable form suitable for display in CREATE FUNCTION statements. The function specifically excludes procedures (which don't have return types) and returns NULL for them.

## Parameters / Member Variables
- `funcid`: OID of the target function whose return type is to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - PROKIND_PROCEDURE
  - print_function_rettype
  - string_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Returns NULL if the function OID is invalid or the function doesn't exist
- Returns NULL for procedures (prokind = PROKIND_PROCEDURE) since procedures don't have return types
- Uses the print_function_rettype helper function to generate the formatted output
- Part of the ruleutils.c module which provides functions for formatting SQL statements and database object definitions
- Essential for displaying function signatures and generating CREATE FUNCTION statements
- The function distinguishes between functions (which have return types) and procedures (which do not)