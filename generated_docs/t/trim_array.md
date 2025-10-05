# trim_array

## Location
[src/backend/utils/adt/arrayfuncs.c:6910-6949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6910-L6949)

## Overview
Trims the last N elements from a PostgreSQL array by creating an appropriate slice from the original array, operating only on the first dimension.

## Definition

```c
Datum
trim_array(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL  SQL function, which removes a specified number of elements from the end of an array. It works by constructing slice bounds and using the existing  infrastructure to create a new array with the desired elements.

The function only operates on the first dimension of multi-dimensional arrays, leaving other dimensions unchanged. It performs bounds checking to ensure the number of elements to trim is valid (between 0 and the array length), raising an error if the request is out of bounds.

The implementation leverages PostgreSQL's array slicing mechanism by setting up appropriate upper and lower bounds, with only the upper bound of the first dimension being specified to define where the trimming should occur.

## Parameters / Member Variables
-  (PG_GETARG_ARRAYTYPE_P(0)): The input array to be trimmed
-  (PG_GETARG_INT32(1)): Number of elements to remove from the end (must be between 0 and array length)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - PG_GETARG_INT32
  - ARR_NDIM
  - ARR_DIMS
  - ARR_LBOUND
  - ARR_ELEMTYPE
  - MAXDIM
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [array_get_slice](../a/array_get_slice.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - PG_RETURN_DATUM
- Called from:
  - This appears to be a top-level SQL function implementation

## Notes and Other Information
- Only trims from the first dimension of the array; other dimensions remain unaffected
- Validates that the number of elements to trim is within valid bounds (0 to array_length)
- Uses memset to initialize bound-related arrays to false/unprovided state
- Calculates the new upper bound as: 
- Retrieves element type information (length, pass-by-value, alignment) to properly handle the array data
- Delegates the actual array construction to  for consistency with PostgreSQL's slicing infrastructure
- Returns a new array datum rather than modifying the input array in place
- Handles empty arrays gracefully (array_length = 0 when ARR_NDIM(v) = 0)

## Simplified Source

```c
Datum
trim_array(PG_FUNCTION_ARGS)
{
    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    int n = PG_GETARG_INT32(1);
    int array_length = (ARR_NDIM(v) > 0) ? ARR_DIMS(v)[0] : 0;

    // Validate trim count is within bounds
    if (n < 0 || n > array_length)
        ereport(ERROR,
                (errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
                 errmsg("number of elements to trim must be between 0 and %d",
                        array_length)));

    // Setup slice bounds - initialize all as unprovided
    int lower[MAXDIM], upper[MAXDIM];
    bool lowerProvided[MAXDIM], upperProvided[MAXDIM];
    memset(lowerProvided, false, sizeof(lowerProvided));
    memset(upperProvided, false, sizeof(upperProvided));

    // Set upper bound for first dimension to trim last n elements
    if (ARR_NDIM(v) > 0) {
        upper[0] = ARR_LBOUND(v)[0] + array_length - n - 1;
        upperProvided[0] = true;
    }

    // Get element type information
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(ARR_ELEMTYPE(v), &elmlen, &elmbyval, &elmalign);

    // Create trimmed array using slice operation
    Datum result = array_get_slice(PointerGetDatum(v), 1,
                                   upper, lower, upperProvided, lowerProvided,
                                   -1, elmlen, elmbyval, elmalign);

    PG_RETURN_DATUM(result);
}
```