# sort_pool

## Location
[src/backend/optimizer/geqo/geqo_pool.c:135-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L135-L144)

## Overview
Sorts a genetic algorithm pool of chromosomes in ascending order by their fitness scores (worth values) using the standard qsort function.

## Definition
```c
void sort_pool(PlannerInfo *root, Pool *pool)
```

## Detailed Description
The `sort_pool` function provides a simple wrapper around the standard C library `qsort` function to sort chromosomes in a genetic algorithm pool based on their fitness values. The sorting is performed in ascending order, meaning chromosomes with lower cost (better fitness) appear first in the array.

This sorting is essential for genetic algorithm operations such as selection, where the best-performing chromosomes (those with lowest cost) need to be easily identified and preferentially chosen for reproduction. The function uses a custom comparison function `compare` that understands how to compare Chromosome structures based on their worth field.

## Parameters / Member Variables
- `root`: PlannerInfo pointer providing planning context (though not directly used in this function)
- `pool`: Pointer to the Pool structure containing chromosomes to be sorted

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard C library sorting function)
  - [compare](../c/compare.md) (custom comparison function for Chromosome structures)
  - Pool (struct type for genetic algorithm pool)
  - Chromosome (struct type for individual solutions)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO optimization function for population management)

## Notes and Other Information
- Sorts chromosomes in ascending order by worth (fitness) values, with best solutions first
- Essential for selection operations in genetic algorithms where fitter individuals are preferentially chosen
- Uses the standard qsort algorithm which provides O(n log n) average time complexity
- The comment suggests that the compare() function can be modified to change sorting behavior if needed
- Part of PostgreSQL's genetic query optimizer population management system