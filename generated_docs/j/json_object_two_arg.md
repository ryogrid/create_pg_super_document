# json_object_two_arg

## Location
src/backend/utils/adt/json.c: 1485 - 1562

## Overview
SQL function that constructs JSON objects from two separate PostgreSQL text arrays, one for keys and one for values.

## Definition


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
  - deconstruct_array_builtin
  - TextDatumGetCString
  - escape_json
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