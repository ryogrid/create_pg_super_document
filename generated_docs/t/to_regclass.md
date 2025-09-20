# to_regclass

## Location
[src/backend/utils/adt/regproc.c:925-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L925-L942)

## Overview
Converts a class name to class OID with NULL return on failure, providing a safe alternative to regclass input conversion.

## Definition

```c
Datum
to_regclass(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a "safe" version of regclass input conversion in PostgreSQL. Unlike the  function which throws an error when a class name is not found,  returns NULL instead, making it suitable for cases where the existence of a relation is uncertain and error handling is preferred to be done by the caller.

The function accepts a text input containing a class name (potentially schema-qualified) and attempts to convert it to the corresponding relation OID. It uses the same underlying logic as  but wraps the call in error handling infrastructure that catches lookup failures and converts them to NULL returns rather than throwing exceptions.

This function is commonly used in SQL queries where you want to check if a table exists without causing an error, such as in conditional logic or validation scenarios.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - Argument 0:  - The class name (as PostgreSQL text type) to convert to OID

## Dependencies
- Functions called/Symbols referenced:
  -  - Convert PostgreSQL text type to C string
  -  - Extract text argument with possible detoasting
  -  - Error context structure for safe function calls
  -  - Safely call input function with error handling
  -  - The underlying regclass input function
  -  - Constant for invalid OID
  -  - Return NULL from function
  -  - Return datum from function

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Primary difference from : returns NULL on failure instead of throwing an error
- Uses  to safely invoke  with error context
- The ErrorSaveContext structure captures any errors that occur during the regclassin call
- Commonly used in SQL for existence checks: 
- Part of PostgreSQL's "to_reg*" family of functions that provide safe alternatives to reg* input functions
- Accepts same input formats as regclassin: simple names, schema-qualified names, numeric OIDs, and dash ("-")
- Returns the same OID values as regclassin when successful, but NULL instead of error on failure