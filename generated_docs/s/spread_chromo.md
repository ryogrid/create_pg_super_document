# spread_chromo

## Location
[src/backend/optimizer/geqo/geqo_pool.c:187-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L187-L265)

## Overview
The  function inserts a new chromosome into the gene pool by displacing the worst chromosome, maintaining the sorted order of chromosomes from best to worst fitness.

## Definition

```c
void
spread_chromo(PlannerInfo *root, Chromosome *chromo, Pool *pool)
```
## Detailed Description
This function implements a crucial operation in the GEQO genetic algorithm by inserting a new chromosome into the population pool while maintaining the sorted order based on fitness (worth). The function uses a binary search algorithm to find the appropriate insertion position for the new chromosome, then shifts existing chromosomes to make room for the insertion.

The function assumes that the pool is sorted from best to worst (smallest to largest worth values). If the new chromosome is worse than the worst chromosome in the pool, it is rejected. Otherwise, the function performs a binary search to find the correct insertion point and then shifts chromosomes accordingly, always replacing the worst chromosome in the pool.

The algorithm ensures that the pool maintains its sorted order and fixed size, implementing a key component of the selection pressure in the genetic algorithm.

## Parameters / Member Variables
- : PlannerInfo pointer representing the planner context
- : Pointer to the new Chromosome to be inserted into the pool
- : Pointer to the Pool structure containing the chromosome population

## Dependencies
- Functions called/Symbols referenced:
  -  (function to copy chromosome data)
  -  (structure type)
  -  (structure type for the chromosome pool)
- Called from (representative examples):
  -  function in geqo_main.c (during genetic algorithm execution)

## Notes and Other Information
- Uses binary search for efficient insertion position finding in O(log n) time
- Maintains pool sorted order (best to worst fitness)
- Rejects chromosomes worse than the current worst chromosome in the pool
- Implements chromosome shifting to maintain pool size constraints
- Critical for maintaining selection pressure in the genetic algorithm
- The function is declared in 
- [Pool](../P/Pool.md) assumes fitness values where smaller worth indicates better fitness