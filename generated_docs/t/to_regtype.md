# to_regtype

## Location
[src/backend/utils/adt/regproc.c:1209-1228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1209-L1228)

## Overview
Converts a text string representation of a type name to its corresponding regtype value, returning NULL if the type name is not found instead of raising an error.

## Definition

```c
Datum
to_regtype(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides a safe, non-error-raising alternative to direct type name to regtype conversion. Unlike  which throws an error for invalid type names,  returns NULL when a type name cannot be resolved. This makes it suitable for use in queries where type existence needs to be tested or where error handling should be managed at the SQL level.

The function workflow:
1. **Input conversion**: Converts the input text argument to a C string
2. **Safe parsing**: Uses  with an error save context to call 
3. **Error handling**: If  would raise an error (invalid type name), the function catches it and returns NULL instead
4. **Success path**: If parsing succeeds, returns the resulting regtype OID value

This function is particularly useful in SQL contexts where conditional type checking is needed without triggering query failures.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  - : Convert PostgreSQL TEXT to C string
  - : Extract text argument with potential detoasting
  - : Error context structure for safe error handling
  - : Safely call input function with error catching
  - : Core function for type name to OID conversion
  - : Return NULL Datum when type is not found
  - : Return successful conversion result
- Called from:
  - SQL queries as a built-in function (available to end users)

## Notes and Other Information
- This function is the "safe" variant of regtype input conversion - it never raises errors for invalid input
- The NULL return behavior makes it ideal for use in CASE statements, WHERE clauses, and other conditional SQL contexts
- Internally leverages the same parsing logic as  but with error suppression
- Part of PostgreSQL's suite of "to_reg*" functions that provide error-safe object name resolution
- The function supports all the same input formats as  including complex type syntax, schema qualification, and array notation

## Simplified Source

```c
Datum
to_regtype(PG_FUNCTION_ARGS)
{
    // Convert input text to C string
    char *type_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

    // Set up error handling context
    ErrorSaveContext escontext = {T_ErrorSaveContext};

    // Try to convert type name to OID safely
    Datum result;
    if (!DirectInputFunctionCallSafe(regtypein, type_name,
                                   InvalidOid, -1,
                                   (Node *) &escontext, &result)) {
        // Return NULL if conversion fails
        PG_RETURN_NULL();
    }

    // Return successful conversion result
    PG_RETURN_DATUM(result);
}
```