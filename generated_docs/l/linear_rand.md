# linear_rand

## Location
[src/backend/optimizer/geqo/geqo_selection.c:88-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_selection.c#L88-L111)

## Overview
Generates a biased random integer index using a linear probability distribution to implement selection pressure in genetic algorithms.

## Definition
```c
static int linear_rand(PlannerInfo *root, int pool_size, double bias)
```

## Detailed Description
This function implements biased random selection using a linear probability distribution function. It generates random integers between 0 and pool_size-1, where lower indices have higher probability of being selected based on the bias parameter. This is used in genetic algorithms to implement selection pressure that favors better-performing individuals.

The probability distribution function is: f(x) = bias - 2(bias - 1)x, where bias represents the ratio of probability of the first element to the probability of the middle element. The function uses the inverse transform sampling method to generate random numbers following this distribution.

The implementation includes safeguards against numerical issues, including handling edge cases where the square root calculation might produce invalid results, and ensuring the generated index stays within the valid range [0, pool_size).

## Parameters / Member Variables
- `root`: PlannerInfo context containing query planning information and random state
- `pool_size`: Maximum size of the pool (generated index will be 0 to pool_size-1)
- `bias`: Linear bias factor controlling selection pressure (bias = prob_first / prob_middle)

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_rand](../g/geqo_rand.md) (provides uniform random numbers between 0.0 and 1.0)
  - sqrt (standard math library square root function)
- Called from (representative examples):
  - [geqo_selection](../g/geqo_selection.md) (uses this for biased parent selection)

## Notes and Other Information
- This is a static function, only accessible within geqo_selection.c
- Uses inverse transform sampling to convert uniform random numbers to linear distribution
- Includes robust error handling for numerical edge cases and roundoff errors
- Higher bias values create stronger selection pressure toward lower indices
- The function may retry the calculation if numerical issues produce out-of-range results
- Part of PostgreSQL's Genetic Query Optimizer (GEQO) selection mechanism