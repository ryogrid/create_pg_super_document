# ArrayGetNItemsSafe

## Location
[src/backend/utils/adt/arrayutils.c:67-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L67-L116)

## Overview
Safely calculates the total number of elements in a multidimensional array with overflow checking and optional soft error handling.

## Definition
```c
int ArrayGetNItemsSafe(int ndim, const int *dims, struct Node *escontext)
```

## Detailed Description
ArrayGetNItemsSafe performs the core calculation of total array elements by multiplying dimension sizes together, with comprehensive overflow protection. It can either throw exceptions or return errors through an ErrorSaveContext, making it suitable for both internal operations and user-facing functions that need graceful error handling.

The function implements several safety checks: it validates that dimensions are non-negative (negative values indicate previous overflow), performs 64-bit intermediate arithmetic to detect multiplication overflow, and ensures the final result doesn't exceed MaxArraySize. These protections prevent integer overflow attacks and system instability from maliciously large array requests.

The algorithm uses int64 arithmetic for intermediate calculations, then validates that the result fits within int32 bounds and PostgreSQL's maximum array size limits.

## Parameters / Member Variables
- `ndim`: Number of dimensions in the array  
- `dims`: Array of dimension sizes to multiply together
- `escontext`: Error context for soft error handling (NULL for exception throwing)

## Dependencies
- Functions called/Symbols referenced:
  - ereturn (error handling macro)
  - MaxArraySize (system constant)
- Called from (representative examples):
  - [ArrayGetNItems](ArrayGetNItems.md)
  - AARR_LBOUND (array header macro)

## Notes and Other Information
- Returns -1 on error when using ErrorSaveContext, otherwise throws exceptions
- Performs overflow checking using int64 intermediate arithmetic  
- Validates against PostgreSQL's MaxArraySize limit
- Negative dimension values indicate previous UB-LB overflow and trigger errors
- Core safety function for preventing array size overflow vulnerabilities
- Designed to work on platforms with int64 arithmetic support