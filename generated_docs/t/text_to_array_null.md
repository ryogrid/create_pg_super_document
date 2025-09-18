# text_to_array_null

## Location
src/backend/utils/adt/varlena.c: 4540 - 4550

## Overview
A wrapper function for text_to_array that handles null string parameters in text array splitting operations.

## Definition
```c
Datum text_to_array_null(PG_FUNCTION_ARGS)
```

## Detailed Description
The text_to_array_null function is a separate entry point that delegates directly to text_to_array. It exists primarily to provide a distinct function signature for cases where null string handling is explicitly required in text array operations. The function serves as a compatibility layer to prevent regression test complaints about different argument sets for the same internal functionality.

## Parameters / Member Variables
- Takes PostgreSQL function arguments via PG_FUNCTION_ARGS macro (typically text input string, delimiter, and null string parameter)
- Returns a Datum representing the resulting text array

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_array](text_to_array.md) (core text-to-array conversion function)
- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:4540-4550
- This is essentially a pass-through function that exists for API completeness
- Created specifically to prevent regression test issues with different argument sets
- Part of PostgreSQL's variable-length data type utilities