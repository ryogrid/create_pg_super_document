# ArrayCheckBounds

## Location
[src/backend/utils/adt/arrayutils.c:117-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L117-L126)

## Overview
Validates that array lower-bound values will not cause integer overflow when calculating array subscripts.

## Definition
```c
void ArrayCheckBounds(int ndim, const int *dims, const int *lb)
```

## Detailed Description
ArrayCheckBounds verifies the sanity of proposed lower-bound values for array dimensions to prevent overflow when calculating array subscripts. It ensures that the combination of dimension size and lower bound values will not exceed integer limits during subscript arithmetic operations.

The function specifically checks that dims[i] + lb[i] can be computed without overflow for each dimension. This prevents scenarios where extremely large lower bounds combined with dimension sizes could cause integer wraparound, leading to incorrect memory access or security vulnerabilities.

This is a convenience wrapper around ArrayCheckBoundsSafe that throws exceptions on validation failures rather than providing soft error handling. It assumes that dimension validation (via ArrayGetNItems) has already been performed to eliminate negative dimension values.

## Parameters / Member Variables
- `ndim`: Number of dimensions in the array
- `dims`: Array of dimension sizes for each dimension
- `lb`: Array of lower bound values to validate

## Dependencies
- Functions called/Symbols referenced:
  - ArrayCheckBoundsSafe
- Called from (representative examples):
  - ExecEvalArrayExpr
  - array_cat
  - array_recv
  - array_set_element
  - array_set_element_expanded
  - array_set_slice
  - construct_md_array
  - makeArrayResultArr
  - array_fill_internal

## Notes and Other Information
- Wrapper around ArrayCheckBoundsSafe that throws exceptions instead of returning errors
- Assumes ArrayGetNItems has already validated dimensions to eliminate negative values
- Prevents arrays with last subscript equal to INT_MAX to avoid overflow
- Critical for preventing subscript calculation overflow in array operations
- Used during array creation and modification to ensure safe bounds