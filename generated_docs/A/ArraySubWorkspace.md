# ArraySubWorkspace

## Location
src/backend/utils/adt/arraysubs.c: 29 - 45

## Overview
ArraySubWorkspace is a workspace structure used during array subscripting operations in PostgreSQL, containing cached type information and converted subscript indices for efficient array element access.

## Definition


## Detailed Description
ArraySubWorkspace serves as a specialized workspace structure for PostgreSQL's array subscripting execution framework. It is allocated and populated during the setup phase of array subscript operations and contains both cached type system information and pre-converted subscript indices. The structure is designed to optimize array access operations by pre-computing and storing frequently needed metadata about the array and element types, avoiding repeated lookups during execution.

The workspace is part of the SubscriptingRefState.workspace mechanism, which allows different subscriptable types (arrays, jsonb, etc.) to maintain their own execution-specific data structures. This design provides type-specific optimization while maintaining a common subscripting interface.

## Parameters / Member Variables
- : OID identifying the PostgreSQL type of individual array elements
- : The storage length (typlen) of the array type itself
- : The storage length (typlen) of the array element type
- : Boolean indicating whether array elements are passed by value or by reference
- : Character indicating the alignment requirements (typalign) of the element type
- : Array of converted upper bound indices for subscript operations, sized to maximum dimensions (6)
- : Array of converted lower bound indices for slice operations, sized to maximum dimensions (6)

## Dependencies
- Functions called/Symbols referenced:
  - MAXDIM (constant defining maximum array dimensions as 6)
- Called from (representative examples):
  - array_subscript_check_subscripts
  - array_subscript_fetch
  - array_subscript_fetch_slice
  - array_subscript_assign
  - array_subscript_assign_slice
  - array_subscript_fetch_old
  - array_subscript_fetch_old_slice
  - array_exec_setup

## Notes and Other Information
The index arrays are always allocated with MAXDIM (6) entries regardless of the actual number of subscripts being used, because internal array manipulation functions like array_get_slice and array_set_slice may write to unused entries. This design choice prioritizes safety and simplicity over memory efficiency. The workspace is allocated using PostgreSQL's memory context system (palloc) and is automatically cleaned up when the query execution context ends.