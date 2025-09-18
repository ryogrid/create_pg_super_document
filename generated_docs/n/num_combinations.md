# num_combinations

## Location
[src/backend/statistics/mvdistinct.c:575-588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L575-L588)

## Overview
Calculates the number of combinations, excluding single-value combinations.

## Definition
```c
static int num_combinations(int n)
```

## Detailed Description
This function computes the total number of possible combinations for n elements, excluding combinations with only one element. It uses a bit manipulation approach where (1 << n) gives 2^n (all possible subsets including empty set), then subtracts (n + 1) to exclude the empty set and all single-element combinations. The result represents the number of meaningful multi-element combinations that can be formed from n elements.

## Parameters / Member Variables
- `n`: The number of elements to form combinations from

## Dependencies
- Functions called/Symbols referenced:
  - [CombinationGenerator](../C/CombinationGenerator.md) (referenced at line 588)
- Called from (representative examples):
  - statext_ndistinct_build

## Notes and Other Information
- The function is declared as static, limiting its scope to mvdistinct.c
- Uses bit shifting (1 << n) for efficient computation of 2^n
- The formula (2^n - (n + 1)) effectively excludes the empty set and all n single-element subsets
- Located in src/backend/statistics/mvdistinct.c, indicating its use in multivariate distinct value statistics
- The result represents combinations of size 2 or greater from n elements