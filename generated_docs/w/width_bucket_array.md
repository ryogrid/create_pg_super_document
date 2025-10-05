# width_bucket_array

## Location
[src/backend/utils/adt/arrayfuncs.c:6678-6740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6678-L6740)

## Overview
Implements the width_bucket(anyelement, anyarray) function that assigns a bucket number to an operand value based on an array of threshold values.

## Definition

```c
Datum
width_bucket_array(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL width_bucket function variant that takes an element and an array of thresholds. It determines which "bucket" the operand falls into based on the threshold values provided in the array. The thresholds array must be sorted from smallest to largest to produce correct results.

The function returns:
- 0 for inputs less than the first threshold
- N for inputs greater than or equal to the last threshold (where N is the number of thresholds)
- Values 1 through N-1 for inputs that fall between consecutive thresholds

The function includes optimizations for different data types:
- A dedicated fast path for float8 (double precision) data
- Separate implementations for fixed-width and variable-width data types

## Parameters / Member Variables
-  (PG_GETARG_DATUM(0)): The value to be bucketed
-  (PG_GETARG_ARRAYTYPE_P(1)): One-dimensional array of threshold values, must be sorted and contain no NULLs

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - PG_GET_COLLATION  
  - ARR_ELEMTYPE
  - ARR_NDIM
  - [array_contains_nulls](../a/array_contains_nulls.md)
  - [width_bucket_array_float8](width_bucket_array_float8.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [width_bucket_array_fixed](width_bucket_array_fixed.md)
  - [width_bucket_array_variable](width_bucket_array_variable.md)
  - TYPECACHE_CMP_PROC_FINFO
- Called from:
  - This appears to be a top-level SQL function implementation

## Notes and Other Information
- The thresholds array must be one-dimensional, otherwise an error is raised
- NULL values in the thresholds array are not permitted
- The function uses type caching to optimize repeated calls with the same data type
- Memory management includes freeing toasted input to avoid leaks
- For float8 data, uses a specialized implementation for better performance
- For other types, chooses between fixed-width and variable-width implementation paths based on the type's storage characteristics

## Simplified Source

```c
Datum
width_bucket_array(PG_FUNCTION_ARGS)
{
    Datum operand = PG_GETARG_DATUM(0);
    ArrayType *thresholds = PG_GETARG_ARRAYTYPE_P(1);
    Oid collation = PG_GET_COLLATION();
    Oid element_type = ARR_ELEMTYPE(thresholds);

    // Validate input array
    if (ARR_NDIM(thresholds) > 1)
        ereport(ERROR, "thresholds must be one-dimensional array");

    if (array_contains_nulls(thresholds))
        ereport(ERROR, "thresholds array must not contain NULLs");

    int result;

    // Use optimized path for float8
    if (element_type == FLOAT8OID) {
        result = width_bucket_array_float8(operand, thresholds);
    } else {
        // Get or cache comparison function for element type
        TypeCacheEntry *typentry = get_cached_comparison_proc(fcinfo, element_type);

        // Choose implementation based on type characteristics
        if (typentry->typlen > 0) {
            // Fixed-width types
            result = width_bucket_array_fixed(operand, thresholds, collation, typentry);
        } else {
            // Variable-width types
            result = width_bucket_array_variable(operand, thresholds, collation, typentry);
        }
    }

    // Clean up toasted input
    PG_FREE_IF_COPY(thresholds, 1);

    PG_RETURN_INT32(result);
}
```