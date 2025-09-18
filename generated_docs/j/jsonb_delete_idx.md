# jsonb_delete_idx

## Location
src/backend/utils/adt/jsonfuncs.c: 4780 - 4843

## Overview
Deletes an element from a JSONB array by its index, supporting both positive and negative indices.

## Definition


## Detailed Description
The  function is a SQL-callable function that removes an element at a specified index from a JSONB array. It accepts positive indices (counting from the beginning) and negative indices (counting backward from the end). The function validates that the input is an array (not a scalar or object), handles edge cases like out-of-bounds indices gracefully, and returns a new JSONB array with the specified element removed.

The function creates a new JSONB value rather than modifying the input in place, following PostgreSQL's immutable data structure approach. It uses the JSONB iterator mechanism to traverse the array elements and rebuilds the array while skipping the target element.

## Parameters / Member Variables
- : The input JSONB array from which to delete an element
- : The index of the element to delete. Negative values count from the end

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_GETARG_INT32: Extract integer argument from function call
  - JB_ROOT_IS_SCALAR: Check if JSONB root is a scalar value
  - JB_ROOT_IS_OBJECT: Check if JSONB root is an object
  - JB_ROOT_COUNT: Get the count of elements in JSONB root
  - JsonbIteratorInit: Initialize JSONB iterator
  - JsonbIteratorNext: Get next element from JSONB iterator
  - pushJsonbValue: Add value to JSONB parse state
  - JsonbValueToJsonb: Convert JsonbValue to Jsonb
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- Only works with JSONB arrays; returns error for scalars and objects
- Negative indices are supported (e.g., -1 for last element)
- Out-of-bounds indices return the original array unchanged
- Empty arrays return the original array unchanged
- Uses PostgreSQL's iterator pattern for efficient JSONB traversal
- File location: src/backend/utils/adt/jsonfuncs.c:4780-4843