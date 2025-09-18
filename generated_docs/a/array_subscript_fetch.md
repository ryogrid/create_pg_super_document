# array_subscript_fetch

## Location
src/backend/utils/adt/arraysubs.c: 236 - 263

## Overview
Evaluates a SubscriptingRef fetch operation for a single array element, extracting the specified element from the source array using pre-computed integer subscripts.

## Definition


## Detailed Description
This function performs the actual array element retrieval during expression execution. It is called after subscripts have been validated and converted to integers by array_subscript_check_subscripts. The function operates on a non-NULL source array (enforced by setting fetch_strict to true) and uses the pre-computed subscripts stored in the workspace to extract a single element.

The function delegates the core array access logic to array_get_element, which handles the low-level details of navigating PostgreSQL's array storage format. The result is stored directly in the operation's result value, and the result null indicator is updated appropriately by array_get_element if the accessed element is out of bounds or otherwise invalid.

## Parameters / Member Variables
- : Expression state context (not directly used in this function)  
- : Expression evaluation step containing the SubscriptingRef state, workspace, and result storage locations
- : Expression evaluation context (not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - array_get_element (performs the actual array element extraction using the computed subscripts and type information)
- Called from (representative examples):
  - array_exec_setup (configures this function as part of the expression evaluation sequence)

## Notes and Other Information
- This is a static function internal to the array subscripting module
- Designed specifically for single element fetches (not array slices)
- Assumes source array and subscripts are already validated as non-NULL
- Part of PostgreSQL's expression evaluation framework, following the ExprEvalStep function signature pattern
- Works in conjunction with array_subscript_check_subscripts which must be called first to prepare the workspace
- The Assert statement documents the assumption that NULL handling has been done in prior steps
- Result handling (including NULL detection for out-of-bounds access) is delegated to array_get_element
- Performance-optimized by pre-validating inputs and using workspace storage for subscripts