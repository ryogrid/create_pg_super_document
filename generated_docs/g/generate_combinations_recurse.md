# generate_combinations_recurse

## Location
[src/backend/statistics/mvdistinct.c:657-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L657-L691)

## Overview
Recursively generates all possible k-combinations of n elements in lexicographic order, used as the core recursive function for multivariate distinct statistics computation in PostgreSQL.

## Definition

```c
static void
generate_combinations_recurse(CombinationGenerator *state,
							  int index, int start, int *current)
```
## Detailed Description
This function implements the recursive logic for generating all possible combinations of k elements from n total elements. It operates by filling combination positions one by one, ensuring lexicographic ordering to eliminate duplicate permutations. The function uses a depth-first recursive approach where each recursive call fills one position in the combination array.

The algorithm ensures that combinations are generated in ascending order by constraining each subsequent element to be greater than the previous one. When all k positions are filled (index == k), the complete combination is stored in the generator's combinations array.

This function is part of PostgreSQL's multivariate distinct statistics system, specifically used for estimating the number of distinct combinations of column values.

## Parameters / Member Variables
- : Pointer to CombinationGenerator structure containing the generation state and result storage
- : Current position being filled in the combination (0-indexed)
- : Minimum value that can be placed at the current position (ensures ascending order)
- : Working array storing the combination being built

## Dependencies
- Functions called/Symbols referenced:
  - [CombinationGenerator](../C/CombinationGenerator.md) (structure)
  - [generate_combinations_recurse](generate_combinations_recurse.md) (recursive self-call)
  - memcpy (for copying completed combinations)

- Called from (representative examples):
  - [generate_combinations_recurse](generate_combinations_recurse.md) (recursive calls)
  - [generate_combinations](generate_combinations.md)

## Notes and Other Information
- Uses lexicographic ordering to avoid generating duplicate permutations
- Implements classic combinatorial generation algorithm with backtracking
- Part of PostgreSQL's extended statistics infrastructure for query optimization
- Memory management is handled by the caller (generate_combinations function)
- The recursive depth equals k (combination size), making it suitable for small to moderate k values
- Critical for multivariate distinct value estimation in complex query planning