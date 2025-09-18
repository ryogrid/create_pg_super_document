# array_position_start

## Location
[src/backend/utils/adt/array_userfuncs.c:1231-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1231-L1243)

## Overview
A PostgreSQL user function that finds the position of an element in an array, starting from the beginning of the array.

## Definition


## Detailed Description
 is a simple wrapper function for the  SQL function. It delegates all its functionality to the common implementation . This function searches for a specified element within a one-dimensional array and returns the 1-based index of the first occurrence. The function is designed to work with the PostgreSQL function call interface and handles null inputs appropriately.

The function is part of PostgreSQL's array utility functions and provides a way to locate elements within arrays. It only operates on one-dimensional arrays and will reject multi-dimensional arrays with an error.

## Parameters / Member Variables
- : Function call information structure containing:
  - : The input array to search in
  - : The element to search for
  - Optional third parameter for starting position (handled by array_position_common)

## Dependencies
- Functions called/Symbols referenced:
  - [array_position_common](array_position_common.md)
- Called from (representative examples):
  - SQL function  (indirectly through function catalog)

## Notes and Other Information
- This is a separate wrapper function maintained for the sake of the opr_sanity regression test
- The actual implementation logic resides in 
- Returns NULL if the element is not found or if the input array is NULL
- Only supports one-dimensional arrays; multi-dimensional arrays will cause an error
- Uses PostgreSQL's function call interface (PG_FUNCTION_ARGS)
- Located in src/backend/utils/adt/array_userfuncs.c:1231-1243