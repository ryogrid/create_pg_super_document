# array_replace

## Location
[src/backend/utils/adt/arrayfuncs.c:6649-6677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6649-L6677)

## Overview
A SQL function that replaces all occurrences of a specified search element with a replacement element in an array, returning a new array with the substitutions made.

## Definition

```c
Datum
array_replace(PG_FUNCTION_ARGS)
```
## Detailed Description
The array_replace function provides the SQL-callable interface for element substitution in PostgreSQL arrays. It serves as a wrapper around the array_replace_internal function, configuring it to operate in replacement mode. The function takes an array, a search value, and a replacement value as arguments, and returns a new array where all elements matching the search value (using the element type's equality operator) have been replaced with the replacement value.

The function properly handles NULL inputs and values: if the input array is NULL, it returns NULL. Both the search and replacement values can be NULL, allowing for operations like replacing NULL elements with non-NULL values or vice versa. Unlike array removal operations, replacement can be safely performed on multi-dimensional arrays since it preserves the array structure.

This function is registered in PostgreSQL's system catalogs and is directly accessible via SQL as array_replace(anyarray, anyelement, anyelement).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: The input array to process (anyarray type)
  - Argument 1: The element value to search for (anyelement type)
  - Argument 2: The replacement element value (anyelement type)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (macro for checking NULL arguments)
  - PG_GETARG_ARRAYTYPE_P (macro for extracting array argument)
  - PG_GETARG_DATUM (macro for extracting datum arguments)
  - PG_GET_COLLATION (macro for getting collation info)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_ARRAYTYPE_P (macro for returning array result)
  - [array_replace_internal](array_replace_internal.md)
- Called from (representative examples):
  - SQL queries using array_replace(array, search_element, replace_element) syntax
  - PostgreSQL system catalog function invocations

## Notes and Other Information
- This is a SQL-callable function registered in pg_proc system catalog
- Works with both one-dimensional and multi-dimensional arrays
- Maintains the original array dimensions and bounds in the result
- Uses the default collation for the array's element type during comparisons
- The remove parameter is set to false to enable replacement mode in array_replace_internal
- Performance is optimized by returning the original array unchanged if no elements match the search value
- Both search and replacement values support NULL, enabling flexible NULL handling scenarios

## Simplified Source

```c
Datum
array_replace(PG_FUNCTION_ARGS)
{
    // Handle NULL input array
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    // Extract arguments
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(0);
    Datum search = PG_GETARG_DATUM(1);
    bool search_isnull = PG_ARGISNULL(1);
    Datum replace = PG_GETARG_DATUM(2);
    bool replace_isnull = PG_ARGISNULL(2);

    // Call internal implementation with remove=false
    array = array_replace_internal(array,
                                  search, search_isnull,
                                  replace, replace_isnull,
                                  false,             // replace mode
                                  PG_GET_COLLATION(),
                                  fcinfo);

    PG_RETURN_ARRAYTYPE_P(array);
}
```