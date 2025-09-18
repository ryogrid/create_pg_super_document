# array_position

## Location
src/backend/utils/adt/array_userfuncs.c: 1225 - 1230

## Overview
Returns the position (index) of a specified value within a PostgreSQL array using IS NOT DISTINCT FROM semantics for comparison.

## Definition
Datum array_position(PG_FUNCTION_ARGS)

## Detailed Description
This function is a wrapper around array_position_common that searches for a specified value within an array and returns its 1-based position index. The function implements IS NOT DISTINCT FROM semantics, meaning it can successfully locate NULL values if searching for NULL. The search is performed only on single-dimensional arrays as multi-dimensional array searching is not supported due to the complexity of reporting element locations. If the value is not found, the function returns NULL.

The actual implementation delegates to array_position_common, which handles:
- Input validation (rejecting multi-dimensional arrays)
- NULL value searching capability
- Efficient array iteration using PostgreSQL's array iterator
- Type-specific equality comparison using cached operator information
- Memory management for the array iteration process

## Parameters / Member Variables
- : Function call information structure containing:
  - arg0: The array to search in
  - arg1: The value to search for
- Returns: 1-based position index as INT32 or NULL if not found

## Dependencies
- Functions called/Symbols referenced:
  - array_position_common
- Called from (representative examples):
  - No direct references found (used as SQL function)

## Notes and Other Information
- Only works with single-dimensional arrays (multi-dimensional arrays are rejected)
- Uses IS NOT DISTINCT FROM comparison semantics (can find NULL values)
- Returns NULL if the value is not found in the array
- Part of PostgreSQL's array manipulation function family
- The actual search logic is implemented in array_position_common
- Returns 1-based indexing following SQL array conventions
- Efficiently handles both NULL and non-NULL value searches