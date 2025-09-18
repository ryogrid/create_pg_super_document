# text_to_table_null

## Location
[src/backend/utils/adt/varlena.c:4575-4590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4575-L4590)

## Overview
A wrapper function for text_to_table that handles null string parameters in table-based text splitting operations.

## Definition
```c
Datum text_to_table_null(PG_FUNCTION_ARGS)
```

## Detailed Description
The text_to_table_null function is a separate entry point that delegates directly to text_to_table. Similar to text_to_array_null, it exists primarily to provide a distinct function signature for cases where null string handling is explicitly required in text-to-table operations. The function serves as a compatibility layer to prevent regression test complaints about different argument sets for the same internal functionality, specifically for set-returning function variants.

## Parameters / Member Variables
- Takes PostgreSQL function arguments via PG_FUNCTION_ARGS macro (typically text input string, delimiter, and null string parameter)
- Returns a Datum (actual results are managed through the tuple store infrastructure)

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_table](text_to_table.md) (core text-to-table conversion function)
- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:4575-4590
- This is essentially a pass-through function that exists for API completeness
- Created specifically to prevent regression test issues with different argument sets
- Returns table rows instead of arrays, unlike its text_to_array_null counterpart
- Part of PostgreSQL's variable-length data type utilities for set-returning functions