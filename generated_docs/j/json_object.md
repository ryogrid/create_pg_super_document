# json_object

## Location
src/backend/utils/adt/json.c: 1397 - 1484

## Overview
SQL function that constructs JSON objects from PostgreSQL text arrays containing key-value pairs.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL function that creates JSON objects from one- or two-dimensional text arrays. It accepts arrays in two formats:
1. One-dimensional array with alternating key-value pairs (must have even number of elements)
2. Two-dimensional array with exactly 2 columns (keys in first column, values in second)

The function validates input array dimensions, constructs a JSON object string by escaping keys and values appropriately, and returns the result as a text datum. Null keys are rejected with an error, while null values are converted to JSON null literals.

## Parameters / Member Variables
- Input array accessed via : Text array containing key-value pairs for JSON construction

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM  
  - ARR_DIMS
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
- Enforces strict validation: 1D arrays must have even element count, 2D arrays must have exactly 2 columns
- Null keys are forbidden and trigger ERRCODE_NULL_VALUE_NOT_ALLOWED error
- Null values are converted to JSON null literals
- Uses escape_json function to properly escape both keys and values according to JSON standards
- Memory management includes proper cleanup of temporary datums and string buffers