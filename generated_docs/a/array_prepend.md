# array_prepend

## Location
[src/backend/utils/adt/array_userfuncs.c:176-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L176-L239)

## Overview
PostgreSQL function that pushes an element onto the front of a one-dimensional array, extending the array by one element at the beginning.

## Definition
```c
Datum array_prepend(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL array_prepend functionality, which takes a new element and an existing array, then prepends the element to the beginning of the array. The function is designed to work with one-dimensional arrays only and handles both null and non-null input arrays gracefully.

Unlike array_append, array_prepend takes its arguments in reverse order: the element to prepend is the first argument, and the target array is the second argument. The implementation uses PostgreSQL's expanded array representation for efficiency and includes special handling to maintain the original array's lower bound after insertion.

The function includes overflow protection when calculating the new element index and provides clear error messages for invalid input. After insertion, it readjusts the result's lower bound to match the original array's lower bound, as expected for prepend operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: The element to prepend (can be null)
  - Argument 1: The target array (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_DATUM
  - [fetch_array_arg_replace_nulls](../f/fetch_array_arg_replace_nulls.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [array_set_element](array_set_element.md)
  - [EOHPGetRWDatum](../E/EOHPGetRWDatum.md)
  - PG_RETURN_DATUM
- Called from (representative examples):
  - SQL array_prepend() function calls
  - Internal PostgreSQL array operations

## Notes and Other Information
- Only works with empty arrays (0 dimensions) or one-dimensional arrays
- Arguments are in reverse order compared to array_append (element first, array second)
- Provides overflow protection when calculating array indices using subtraction
- Uses expanded array headers for efficient array manipulation
- Maintains the original array's lower bound after prepending
- Returns a new array datum rather than modifying the input in-place
- Handles null elements gracefully by preserving null values in the result array
- Part of PostgreSQL's array manipulation function suite

## Simplified Source

```c
Datum array_prepend(PG_FUNCTION_ARGS) {
    ExpandedArrayHeader *eah;
    Datum newelem;
    bool isNull;
    Datum result;
    int indx, lb0;
    ArrayMetaState *my_extra;

    // Get new element to prepend (first argument)
    isNull = PG_ARGISNULL(0);
    if (isNull)
        newelem = (Datum) 0;
    else
        newelem = PG_GETARG_DATUM(0);

    // Get array argument (replace null with empty array)
    eah = fetch_array_arg_replace_nulls(fcinfo, 1);

    // Calculate index for new element
    if (eah->ndims == 1) {
        // Prepend to existing 1D array: index = lb[0] - 1
        lb0 = eah->lbound[0];
        if (pg_sub_s32_overflow(lb0, 1, &indx))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("integer out of range")));
    }
    else if (eah->ndims == 0) {
        // First element in empty array
        indx = 1;
        lb0 = 1;
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

    // Readjust lower bound to match original array
    if (eah->ndims == 1) {
        eah->lbound[0] = lb0;
    }

    PG_RETURN_DATUM(result);
}
```