# array_dim_to_jsonb

## Location
src/backend/utils/adt/jsonb.c: 862 - 893

## Overview
Recursively processes a single dimension of a PostgreSQL multi-dimensional array, converting it to JSONB array format.

## Definition
```c
static void array_dim_to_jsonb(JsonbInState *result, int dim, int ndims, int *dims, const Datum *vals,
                               const bool *nulls, int *valcount, JsonTypeCategory tcategory,
                               Oid outfuncoid)
```

## Detailed Description
The `array_dim_to_jsonb` function is a recursive helper function that processes multi-dimensional PostgreSQL arrays for conversion to JSONB format. It works by handling one dimension at a time, starting from the outermost dimension and working inward. 

For each dimension, the function:
1. Creates a JSONB array container using WJB_BEGIN_ARRAY
2. Iterates through all elements in the current dimension
3. If it's the innermost dimension (dim + 1 == ndims), it converts each element to JSONB using datum_to_jsonb_internal
4. If it's not the innermost dimension, it recursively calls itself to process the next inner dimension
5. Closes the array container using WJB_END_ARRAY

The function maintains a running count of values processed through the valcount parameter, ensuring that elements from the flattened vals/nulls arrays are consumed in the correct order corresponding to the array's logical structure.

## Parameters / Member Variables
- `result`: JsonbInState structure to accumulate the conversion result
- `dim`: Current dimension being processed (0-indexed)
- `ndims`: Total number of dimensions in the array
- `dims`: Array containing the size of each dimension
- `vals`: Flattened array of Datum values from the original array
- `nulls`: Flattened array of null flags corresponding to vals
- `valcount`: Pointer to counter tracking current position in vals/nulls arrays
- `tcategory`: JsonTypeCategory indicating the element data type classification
- `outfuncoid`: OID of the output function for the element data type

## Dependencies
- Functions called/Symbols referenced:
  - pushJsonbValue
  - datum_to_jsonb_internal
  - array_dim_to_jsonb (recursive self-call)
- Called from (representative examples):
  - array_dim_to_jsonb (recursive calls)
  - array_to_jsonb_internal

## Notes and Other Information
- This is a static function used internally within jsonb.c for array processing
- The function uses recursion to handle arrays with arbitrary numbers of dimensions
- Elements are processed in row-major order, which is the standard layout for PostgreSQL arrays
- The valcount parameter is passed by reference and modified to track progress through the flattened element arrays
- Each recursive call creates one level of nested JSONB arrays, preserving the original array structure
- The function assumes that dims, vals, and nulls arrays are properly sized and valid