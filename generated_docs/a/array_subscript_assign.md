# array_subscript_assign

## Location
src/backend/utils/adt/arraysubs.c: 294 - 343

## Overview
Evaluates a SubscriptingRef assignment operation for array element assignment, handling both fixed-length and variable-length arrays with appropriate NULL value semantics.

## Definition


## Detailed Description
This function performs array element assignment during expression execution, implementing the assignment portion of array subscripting operations (e.g., ). It handles the complex semantics of PostgreSQL array assignment, including different behaviors for fixed-length versus variable-length arrays and proper NULL handling.

The function implements different strategies based on array type:
- **Fixed-length arrays**: Both the original array and replacement value must be non-NULL, otherwise the operation returns the original array unchanged
- **Variable-length arrays**: NULL original arrays are converted to empty arrays before assignment, allowing creation of singleton arrays from NULL

For variable-length arrays, when the original array is NULL, the function creates an empty zero-dimensional array and then inserts the new element, effectively creating a new array. The assignment operation always produces a non-NULL result array.

## Parameters / Member Variables
- : Expression state context (not directly used in this function)
- : Expression evaluation step containing the SubscriptingRef state, workspace, and result storage locations
- : Expression evaluation context (not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - construct_empty_array (creates a new empty array when the original is NULL for varlena arrays)
  - array_set_element (performs the actual element assignment within the array)
  - PointerGetDatum (converts the empty array pointer to a Datum)
- Called from (representative examples):
  - array_exec_setup (configures this function as part of the expression evaluation sequence for assignment operations)

## Notes and Other Information
- This is a static function internal to the array subscripting module
- Designed specifically for array element assignment operations (not fetches or slices)
- Part of PostgreSQL's expression evaluation framework, following the ExprEvalStep function signature pattern
- Implements different NULL-handling semantics for fixed-length vs. variable-length arrays
- The replacement value and null indicator are stored in the SubscriptingRefState structure
- Assignment operations always produce non-NULL result arrays (even if individual elements are NULL)
- For varlena arrays, can transform NULL arrays into single-element arrays through assignment
- Works in conjunction with array_subscript_check_subscripts which must be called first to prepare subscripts
- Performance-optimized by using workspace storage for type metadata and pre-computed subscripts
- The refattrlength field in the workspace distinguishes between fixed-length and variable-length array types