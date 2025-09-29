# ArrayGetNItems

## Location
[src/backend/utils/adt/arrayutils.c:57-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L57-L66)

## Overview
Calculates the total number of elements in a multidimensional array by multiplying all dimension sizes together.

## Definition
```c
int ArrayGetNItems(int ndim, const int *dims)
```

## Detailed Description
ArrayGetNItems is a convenience wrapper function that calculates the total number of elements in a multidimensional array. It delegates the actual computation to ArrayGetNItemsSafe with NULL error context, which means any overflow errors will be thrown as exceptions rather than being handled gracefully.

This function is essential for validating user-requested array dimensionalities and ensuring they don't exceed PostgreSQL's internal limits. It performs overflow checking to prevent integer overflow when dealing with very large arrays, which is critical for system stability and security.

The function is widely used throughout PostgreSQL's array handling code for memory allocation calculations, bounds checking, and array operations validation.

## Parameters / Member Variables
- `ndim`: Number of dimensions in the array
- `dims`: Array of dimension sizes to multiply together

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayGetNItemsSafe](ArrayGetNItemsSafe.md)
- Called from (representative examples):
  - [ExecEvalArrayExpr](../E/ExecEvalArrayExpr.md)
  - [ExecEvalScalarArrayOp](../E/ExecEvalScalarArrayOp.md)
  - [array_cat](../a/array_cat.md)
  - [array_out](../a/array_out.md)
  - [array_recv](../a/array_recv.md)
  - [array_send](../a/array_send.md)
  - [construct_md_array](../c/construct_md_array.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [array_eq](../a/array_eq.md)
  - [array_cmp](../a/array_cmp.md)

## Notes and Other Information
- Wrapper around ArrayGetNItemsSafe that throws exceptions on overflow rather than returning errors
- Essential for array size validation and memory allocation calculations
- Used extensively throughout PostgreSQL's array manipulation functions
- Overflow checking works on machines with int64 arithmetic (nearly all modern platforms)

## Simplified Source
```c
int ArrayGetNItems(int ndim, const int *dims) {
    // Delegate to ArrayGetNItemsSafe with NULL error context
    // This means overflow errors will be thrown as exceptions
    return ArrayGetNItemsSafe(ndim, dims, NULL);
}
```