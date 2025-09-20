# array_subscript_fetch_slice

## Location
[src/backend/utils/adt/arraysubs.c:264-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arraysubs.c#L264-L293)

## Overview
Evaluates a SubscriptingRef fetch operation for an array slice, extracting a sub-array from the source array using pre-computed upper and lower bound subscripts.

## Definition

```c
struct_empty_array(workspace->refelemtype));
```
## Detailed Description
This function performs array slice extraction during expression execution. It is called after subscripts have been validated and converted to integers by array_subscript_check_subscripts. Unlike array_subscript_fetch which extracts single elements, this function handles slice operations that return sub-arrays containing multiple elements.

The function operates on a non-NULL source array (enforced by setting fetch_strict to true) and uses both upper and lower bounds stored in the workspace to define the slice boundaries. It delegates the core slicing logic to array_get_slice, which handles the complex task of creating a new array containing the requested slice of the original array.

Array slices in PostgreSQL are guaranteed to never return NULL - even an empty or out-of-bounds slice returns a valid (but potentially empty) array, which is why the function doesn't modify the resnull flag.

## Parameters / Member Variables
- : Expression state context (not directly used in this function)
- : Expression evaluation step containing the SubscriptingRef state, workspace, and result storage locations
- : Expression evaluation context (not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [array_get_slice](array_get_slice.md) (performs the actual array slicing operation using upper/lower bounds and type information)
- Called from (representative examples):
  - [array_exec_setup](array_exec_setup.md) (configures this function as part of the expression evaluation sequence for slice operations)

## Notes and Other Information
- This is a static function internal to the array subscripting module
- Designed specifically for array slice operations (not single element access)
- Assumes source array and subscripts are already validated as non-NULL
- Part of PostgreSQL's expression evaluation framework, following the ExprEvalStep function signature pattern
- Works in conjunction with array_subscript_check_subscripts which must be called first to prepare the workspace
- Array slices are guaranteed to return non-NULL results (even empty arrays are valid)
- Uses both upperprovided and lowerprovided flags to handle omitted bounds in slice syntax
- Performance-optimized by pre-validating inputs and using workspace storage for subscripts and type metadata
- The result is always a valid array, potentially empty but never NULL
- Handles multi-dimensional array slicing through the underlying array_get_slice function