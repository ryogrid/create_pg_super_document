# json_object_two_arg

## Location
[src/backend/utils/adt/json.c:1485-1562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1485-L1562)

## Overview
SQL function that constructs JSON objects from two separate PostgreSQL text arrays, one for keys and one for values.

## Definition

```c
struct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);
```
## Detailed Description
The `json_object_two_arg` function is a PostgreSQL SQL function that creates JSON objects from two separate one-dimensional text arrays - one containing keys and another containing corresponding values. It validates that both arrays have the same dimensions and element counts, then constructs a JSON object by pairing elements at matching indices. The function ensures proper JSON formatting by escaping keys and values and handles null values appropriately.

## Parameters / Member Variables
- Key array accessed via `PG_GETARG_ARRAYTYPE_P(0)`: Text array containing JSON object keys
- Value array accessed via `PG_GETARG_ARRAYTYPE_P(1)`: Text array containing JSON object values

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM
  - PG_RETURN_DATUM
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - [escape_json](../e/escape_json.md)
  - cstring_to_text_with_len
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- Returns empty JSON object "{}" for zero-dimensional arrays
- Requires both arrays to be one-dimensional and have matching element counts
- Enforces strict validation: mismatched array dimensions trigger ERRCODE_ARRAY_SUBSCRIPT_ERROR
- Null keys are forbidden and trigger ERRCODE_NULL_VALUE_NOT_ALLOWED error
- Null values in the value array are converted to JSON null literals
- Uses escape_json function to properly escape both keys and values according to JSON standards
- Provides cleaner interface compared to json_object for cases where keys and values are naturally separate
- Includes comprehensive memory management with proper cleanup of all allocated arrays and buffers