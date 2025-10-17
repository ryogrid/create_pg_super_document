# array_append

## Location
[src/backend/utils/adt/array_userfuncs.c:123-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L123-L175)

## Overview
PostgreSQL function that pushes an element onto the end of a one-dimensional array, extending the array by one element.

## Definition
```c
Datum array_append(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL array_append functionality, which takes an existing array and appends a new element to its end. The function is designed to work with one-dimensional arrays only and handles both null and non-null input arrays gracefully.

The implementation uses PostgreSQL's expanded array representation for efficiency. When given a null array, it creates a new single-element array. For existing arrays, it calculates the appropriate index for the new element and uses the array_set_element function to perform the actual insertion.

The function includes overflow protection when calculating the new element index and provides clear error messages for invalid input (such as multi-dimensional arrays).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: The input array (can be null)
  - Argument 1: The element to append (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_array_arg_replace_nulls](../f/fetch_array_arg_replace_nulls.md)
  - PG_ARGISNULL
  - PG_GETARG_DATUM
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - [array_set_element](array_set_element.md)
  - [EOHPGetRWDatum](../E/EOHPGetRWDatum.md)
  - PG_RETURN_DATUM
- Called from (representative examples):
  - SQL array_append() function calls
  - Internal PostgreSQL array operations

## Notes and Other Information
- Only works with empty arrays (0 dimensions) or one-dimensional arrays
- Provides overflow protection when calculating array indices
- Uses expanded array headers for efficient array manipulation
- Returns a new array datum rather than modifying the input in-place
- Handles null elements gracefully by preserving null values in the result array
- Part of PostgreSQL's array manipulation function suite

## Simplified Source

```c
Datum array_append(PG_FUNCTION_ARGS) {
    ExpandedArrayHeader *eah;
    Datum newelem;
    bool isNull;
    Datum result;
    int indx;
    ArrayMetaState *my_extra;

    // Get array argument (replace null with empty array)
    eah = fetch_array_arg_replace_nulls(fcinfo, 0);

    // Get new element to append
    isNull = PG_ARGISNULL(1);
    if (isNull)
        newelem = (Datum) 0;
    else
        newelem = PG_GETARG_DATUM(1);

    // Calculate index for new element
    if (eah->ndims == 1) {
        // Append to existing 1D array: index = lb[0] + dims[0]
        if (pg_add_s32_overflow(eah->lbound[0], eah->dims[0], &indx))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("integer out of range")));
    }
    else if (eah->ndims == 0) {
        // First element in empty array
        indx = 1;
    }
    else {
        // Error: only 0D and 1D arrays supported
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                       errmsg("argument must be empty or one-dimensional array")));
    }

    // Insert the element at the calculated index
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    result = array_set_element(EOHPGetRWDatum(&eah->hdr), 1, &indx, newelem, isNull,
                              -1, my_extra->typlen, my_extra->typbyval, my_extra->typalign);

    PG_RETURN_DATUM(result);
}
```