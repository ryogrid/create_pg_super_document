# array_remove

## Location
[src/backend/utils/adt/arrayfuncs.c:6627-6648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6627-L6648)

## Overview
A SQL function that removes all occurrences of a specified element from a one-dimensional array, returning a new array with matching elements deleted.

## Definition

```c
Datum
array_remove(PG_FUNCTION_ARGS)
```
## Detailed Description
The array_remove function provides the SQL-callable interface for removing elements from PostgreSQL arrays. It serves as a thin wrapper around the array_replace_internal function, configuring it to operate in removal mode. The function takes an array and a search value as arguments, and returns a new array with all elements that match the search value (using the element type's equality operator) removed.

The function handles NULL inputs gracefully: if the input array is NULL, it returns NULL. NULL search values are properly supported, allowing removal of NULL elements from arrays. The function inherits the restriction from array_replace_internal that it cannot be used on multi-dimensional arrays, as removing elements would violate the rectangular array structure requirement.

This function is registered in PostgreSQL's system catalogs and is directly accessible via SQL as array_remove(anyarray, anyelement).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: The input array to process (anyarray type)
  - Argument 1: The element value to search for and remove (anyelement type)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (macro for checking NULL arguments)
  - PG_GETARG_ARRAYTYPE_P (macro for extracting array argument)
  - PG_GETARG_DATUM (macro for extracting datum argument)
  - PG_GET_COLLATION (macro for getting collation info)
  - PG_RETURN_NULL (macro for returning NULL)
  - PG_RETURN_ARRAYTYPE_P (macro for returning array result)
  - [array_replace_internal](array_replace_internal.md)
- Called from (representative examples):
  - SQL queries using array_remove(array, element) syntax
  - PostgreSQL system catalog function invocations

## Notes and Other Information
- This is a SQL-callable function registered in pg_proc system catalog
- Raises an error when used on multi-dimensional arrays due to structural constraints
- Returns the original array type, maintaining element type consistency
- Uses the default collation for the array's element type during comparisons
- The replacement value is set to (Datum) 0 with replace_isnull=true since it's not used in remove mode
- Performance is optimized by returning the original array unchanged if no elements match the search value