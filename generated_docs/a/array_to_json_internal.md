# array_to_json_internal

## Location
[src/backend/utils/adt/json.c:465-511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L465-L511)

## Overview
Converts a PostgreSQL array into JSON array format by extracting array metadata, deconstructing the array into individual elements, and recursively processing dimensions.

## Definition
```c
static void array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds)
```

## Detailed Description
array_to_json_internal serves as the main entry point for converting PostgreSQL arrays to JSON format. It handles the initial setup by extracting array metadata (dimensions, element type, size), determines the appropriate JSON conversion category for the element type, and deconstructcts the array into individual Datum values. For empty arrays, it returns "[]". For non-empty arrays, it calls array_dim_to_json to recursively process each dimension. The function manages memory by freeing the temporary arrays created during deconstruction.

## Parameters / Member Variables
- `array`: PostgreSQL Datum containing the array to convert
- `result`: StringInfo buffer where the JSON output is accumulated
- `use_line_feeds`: Boolean controlling whether to add line feeds for pretty formatting

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP (extract ArrayType from Datum)
  - ARR_ELEMTYPE, ARR_NDIM, ARR_DIMS (array metadata macros)
  - ArrayGetNItems (calculate total number of elements)
  - get_typlenbyvalalign (get type information for element type)
  - json_categorize_type (determine JSON conversion approach)
  - deconstruct_array (extract individual elements and null flags)
  - array_dim_to_json (recursive dimension processing)
  - pfree (memory cleanup)
- Called from (representative examples):
  - datum_to_json_internal
  - array_to_json
  - array_to_json_pretty

## Notes and Other Information
The function optimizes for empty arrays by immediately returning "[]" without further processing. It properly handles PostgreSQL's array storage format by deconstructing the array using the element type's storage characteristics (length, by-value flag, alignment). Memory management is handled carefully with pfree calls to avoid leaks from the temporary element and nulls arrays created by deconstruct_array.