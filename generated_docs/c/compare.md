# compare

## Location
[src/backend/optimizer/geqo/geqo_pool.c:145-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L145-L161)

## Overview
A static comparison function used by qsort to compare two Chromosome structures based on their fitness values (worth) for sorting genetic algorithm pools.

## Definition
```c
static int compare(const void *arg1, const void *arg2)
```

## Detailed Description
The `compare` function implements the standard qsort comparison interface to compare two Chromosome structures based on their worth (fitness) values. It follows the standard comparison function contract:

- Returns 0 if both chromosomes have equal fitness
- Returns 1 if the first chromosome has higher cost (worse fitness)  
- Returns -1 if the first chromosome has lower cost (better fitness)

This comparison logic ensures that when used with qsort, chromosomes are sorted in ascending order by their worth values, placing the best-performing chromosomes (lowest cost) at the beginning of the array. This ordering is essential for genetic algorithm selection processes where fitter individuals are preferentially chosen for reproduction.

## Parameters / Member Variables
- `arg1`: Pointer to the first Chromosome structure to compare (cast from const void*)
- `arg2`: Pointer to the second Chromosome structure to compare (cast from const void*)

## Dependencies
- Functions called/Symbols referenced:
  - [Chromosome](../C/Chromosome.md) (struct type for individual solutions containing worth field)
- Called from (representative examples):
  - [sort_pool](../s/sort_pool.md) (via qsort for sorting genetic algorithm pools)
  - Many other PostgreSQL sorting contexts throughout the codebase

## Notes and Other Information
- Declared as static, limiting its scope to the geqo_pool.c file
- Follows the standard qsort comparison function interface with void* parameters
- The comment in sort_pool suggests this function can be modified to change sorting behavior
- Essential for maintaining sorted populations in genetic algorithm operations
- Part of a broader pattern in PostgreSQL where many comparison functions follow this same interface for various data types