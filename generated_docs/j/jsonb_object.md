# jsonb_object

## Location
[src/backend/utils/adt/jsonb.c:1279-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1279-L1378)

## Overview
SQL function that constructs a JSONB object from a one or two dimensional text array representing name-value pairs.

## Definition

```c
struct_array_builtin(in_array, TEXTOID, &in_datums, &in_nulls, &in_count);
```
## Detailed Description
The  function is a PostgreSQL SQL function that takes either a one-dimensional array with an even number of elements (representing alternating keys and values) or a two-dimensional array with exactly two columns (first column for keys, second for values) and constructs a JSONB object. The function validates array dimensions and ensures proper key-value pairing while handling null values appropriately - null keys are rejected with an error, while null values are converted to JSON null values.

## Parameters / Member Variables
- Input array via : Text array containing key-value pairs in one of two formats:
  - 1D array: [key1, value1, key2, value2, ...]  
  - 2D array: [[key1, value1], [key2, value2], ...]

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract array argument
  -  - Get array dimensions
  -  - Get array dimension sizes
  -  - Extract array elements
  -  - Build JSONB structure
  -  - Convert text datum to C string
  -  - Convert JsonbValue to final JSONB
  - Constants: , , , , , 
- Called from: 
  - SQL queries using the jsonb_object() function

## Notes and Other Information
- Validates array structure: 1D arrays must have even number of elements, 2D arrays must have exactly 2 columns
- Null keys are not permitted and will raise an error
- Null values are converted to JSON null values in the resulting object
- Uses JsonbInState for incremental JSONB construction
- Memory management includes freeing temporary arrays after processing