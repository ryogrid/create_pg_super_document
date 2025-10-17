# to_regoperator

## Location
[src/backend/utils/adt/regproc.c:694-721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L694-L721)

## Overview
Converts an operator name with argument types (in the format "oprname(args)") to the corresponding operator OID, returning NULL if the operator is not found.

## Definition

```c
Datum
to_regoperator(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that safely converts a textual representation of an operator to its internal OID representation. Unlike the direct  function, this function returns NULL instead of throwing an error when the operator cannot be found, making it suitable for cases where the existence of an operator is uncertain.

The function takes a text input in the format "oprname(lefttype,righttype)" or "oprname(righttype)" for unary operators, and attempts to resolve it to the corresponding operator OID. It uses the  mechanism to safely call the underlying  function with error handling.

## Parameters / Member Variables
- Input parameter (via ): Text representation of the operator name and argument types

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](text_to_cstring.md)
  - [DirectInputFunctionCallSafe](../D/DirectInputFunctionCallSafe.md)
  - [regoperatorin](../r/regoperatorin.md)
  - PG_RETURN_DATUM
  - PG_RETURN_NULL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's regtype family of functions that provide safe conversion between text and OID representations
- Uses ErrorSaveContext to handle conversion errors gracefully
- Returns NULL instead of throwing errors, making it suitable for conditional operator lookups
- Located in src/backend/utils/adt/regproc.c:694-721

## Simplified Source

```c
Datum to_regoperator(PG_FUNCTION_ARGS) {
    // Convert text input to C string
    char *operator_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Datum result;
    ErrorSaveContext escontext = {T_ErrorSaveContext};

    // Safely call regoperatorin with error handling
    // Returns false if conversion fails (operator not found)
    if (!DirectInputFunctionCallSafe(regoperatorin, operator_name,
                                    InvalidOid, -1,
                                    (Node *) &escontext,
                                    &result))
        PG_RETURN_NULL();

    PG_RETURN_DATUM(result);
}
```