# jsonb_object_two_arg

## Location
src/backend/utils/adt/jsonb.c: 1379 - 1470

## Overview
SQL function that constructs a JSONB object from two separate text arrays - one containing keys and another containing values.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL function that takes two separate one-dimensional text arrays as arguments: one for keys and one for values. It constructs a JSONB object by pairing elements from the two arrays positionally (first key with first value, second key with second value, etc.). The function validates that both arrays have the same dimensions and element count, ensuring proper key-value pairing while handling null values appropriately.

## Parameters / Member Variables
- Key array via : Text array containing object keys
- Value array via : Text array containing corresponding values

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract array arguments
  -  - Get array dimensions
  -  - Extract array elements
  -  - Build JSONB structure
  -  - Convert text datum to C string
  -  - Convert JsonbValue to final JSONB
  - Constants: , , , , , 
- Called from:
  - SQL queries using the jsonb_object(keys_array, values_array) function

## Notes and Other Information
- Requires both arrays to be one-dimensional with matching element counts
- Null keys are not permitted and will raise an error
- Null values are converted to JSON null values in the resulting object
- More intuitive than the single-array version when keys and values are naturally separate
- Uses JsonbInState for incremental JSONB construction
- Memory management includes freeing all temporary arrays after processing