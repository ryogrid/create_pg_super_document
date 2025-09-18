# to_regrole

## Location
[src/backend/utils/adt/regproc.c:1583-1600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1583-L1600)

## Overview
Converts a role name text value to regrole type, returning NULL for non-existent roles instead of raising errors.

## Definition
```c
Datum to_regrole(PG_FUNCTION_ARGS)
```

## Detailed Description
The to_regrole function provides a safe conversion mechanism from text to regrole type. Unlike the regrolein function which raises errors for non-existent roles, to_regrole takes a more lenient approach by returning NULL when a role name cannot be found.

This function serves as a wrapper around regrolein, using PostgreSQL's error save context mechanism to catch errors and convert them to NULL returns. This makes it suitable for use in queries where you want to handle missing roles gracefully rather than aborting the entire operation.

The function accepts text input (which is converted to a C string) and uses DirectInputFunctionCallSafe to safely invoke regrolein with error handling. If regrolein succeeds, the result is returned; if it fails (due to non-existent role or invalid input), NULL is returned instead.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Input text value representing the role name
  - Function context and metadata
  - Return value storage

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring (converts text input to C string)
  - [ErrorSaveContext](../E/ErrorSaveContext.md) (error handling context structure)
  - [regrolein](../r/regrolein.md) (underlying role name to OID conversion function)
  - [DirectInputFunctionCallSafe](../D/DirectInputFunctionCallSafe.md) (safe function call with error catching)
  - PG_RETURN_DATUM (returns the converted value or NULL)
- Called from (representative examples):
  - No direct references found in the codebase (likely exposed as SQL function)

## Notes and Other Information
- Located in src/backend/utils/adt/regproc.c:1583-1600
- Part of the regrole type conversion function suite
- Provides safe, NULL-returning alternative to regrolein
- Uses ErrorSaveContext to catch and handle conversion errors gracefully
- Commonly used in SQL contexts where NULL handling is preferred over error conditions
- The function name follows PostgreSQL's convention for safe conversion functions (to_* prefix)