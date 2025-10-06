# to_regprocedure

## Location
[src/backend/utils/adt/regproc.c:278-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L278-L298)

## Overview
Converts a procedure name with arguments from text to regprocedure OID, returning NULL instead of throwing an error if the procedure is not found.

## Definition
```c
Datum to_regprocedure(PG_FUNCTION_ARGS)
```

## Detailed Description
The `to_regprocedure` function provides a safe conversion from text to regprocedure OID type. Unlike `regprocedurein`, which throws an error when a procedure is not found, this function returns NULL instead. It serves as a user-friendly wrapper around `regprocedurein` with error handling.

The function:
1. Converts the input text to a C string
2. Sets up an error save context to catch any errors
3. Calls `regprocedurein` through `DirectInputFunctionCallSafe`
4. Returns NULL if the conversion fails, or the resulting OID if successful

This function is typically used in SQL contexts where NULL handling is preferred over exception throwing, such as in CASE statements or WHERE clauses.

## Parameters / Member Variables
- Input parameter (via `PG_GETARG_TEXT_PP(0)`): Text containing procedure signature "name(arg_types)"

## Dependencies
- Functions called/Symbols referenced:
  - `[text_to_cstring](text_to_cstring.md)`: Converts PostgreSQL text type to C string
  - [regprocedurein](../r/regprocedurein.md): Core procedure name resolution function
  - [DirectInputFunctionCallSafe](../D/DirectInputFunctionCallSafe.md): Safe function call wrapper with error context
  - [ErrorSaveContext](../E/ErrorSaveContext.md): Error handling context structure
- Called from (representative examples):
  - No direct references found (likely used via SQL function calls)

## Notes and Other Information
- Provides NULL-returning semantics instead of error-throwing for missing procedures
- Uses PostgreSQL's error save context mechanism for safe error handling
- Commonly used in SQL queries where conditional procedure lookup is needed
- Part of PostgreSQL's "to_" family of conversion functions that return NULL on failure
- The function signature follows PostgreSQL's fmgr (function manager) calling convention

## Simplified Source

```c
Datum to_regprocedure(PG_FUNCTION_ARGS) {
    // Convert input text to C string
    char *pro_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Datum result;

    // Set up error context to catch failures
    ErrorSaveContext escontext = {T_ErrorSaveContext};

    // Safely call regprocedurein - returns false on error
    if (!DirectInputFunctionCallSafe(regprocedurein, pro_name,
                                     InvalidOid, -1,
                                     (Node *) &escontext,
                                     &result))
        PG_RETURN_NULL();

    PG_RETURN_DATUM(result);
}
```