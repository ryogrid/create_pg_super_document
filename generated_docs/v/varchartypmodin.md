# varchartypmodin

## Location
[src/backend/utils/adt/varchar.c:648-655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L648-L655)

## Overview
Processes type modifiers for VARCHAR data types by parsing the input array and validating the length specification.

## Definition

```c
Datum
varchartypmodin(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the type modifier input handler for the VARCHAR data type. It accepts an array of type modifiers (typically containing a length specification) and validates them according to VARCHAR constraints. The function delegates the actual validation logic to , which is shared between VARCHAR and CHAR types. The function ensures that the specified length is within valid bounds (at least 1 and not exceeding MaxAttrSize) and returns the processed type modifier value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention macro that provides access to function arguments through the function call context
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (macro to extract ArrayType argument)
  - [anychar_typmodin](../a/anychar_typmodin.md) (shared validation logic for character types)
  - PG_RETURN_INT32 (macro to return int32 result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's type system infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's type system infrastructure and is typically invoked internally when processing VARCHAR type declarations with length modifiers (e.g., VARCHAR(50))
- The function shares implementation logic with  through the common  helper function
- The returned typmod value includes VARHDRSZ offset for historical compatibility reasons
- Input validation ensures length specifications are within the range [1, MaxAttrSize]