# n_choose_k

## Location
[src/backend/statistics/mvdistinct.c:550-574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L550-L574)

## Overview
Computes binomial coefficients using an algorithm that is both efficient and prevents overflows.

## Definition


## Detailed Description
This function calculates the binomial coefficient "n choose k" (C(n,k)), which represents the number of ways to choose k items from n items without regard to order. The implementation uses an iterative algorithm that multiplies and divides alternately to prevent integer overflow that could occur with the naive factorial-based approach. The function leverages the symmetry property of binomial coefficients where C(n,k) = C(n,n-k) to minimize computation by always computing the smaller of k or n-k.

## Parameters / Member Variables
- : The total number of items to choose from
- : The number of items to choose

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
  - Min (macro to find minimum of two values)
- Called from (representative examples):
  - [generator_init](../g/generator_init.md)

## Notes and Other Information
- The function is declared as static, meaning it's only accessible within the mvdistinct.c compilation unit
- Uses assertion to ensure k > 0 and n >= k, indicating valid input constraints
- The algorithm avoids computing large factorials directly, instead computing the result iteratively
- Located in src/backend/statistics/mvdistinct.c, suggesting it's used for multivariate distinct value statistics calculations
- The symmetry optimization (using Min(k, n-k)) ensures the loop runs at most n/2 times