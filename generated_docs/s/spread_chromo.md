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

## Simplified Source

```c
void
spread_chromo(PlannerInfo *root, Chromosome *chromo, Pool *pool)
{
    int top, mid, bot;
    int i, index;
    Chromosome swap_chromo, tmp_chromo;

    // Reject chromosome if it's worse than the worst in pool
    if (chromo->worth > pool->data[pool->size - 1].worth)
        return;

    // Binary search to find insertion position
    top = 0;
    mid = pool->size / 2;
    bot = pool->size - 1;
    index = -1;

    while (index == -1)
    {
        // Find insertion point
        if (chromo->worth <= pool->data[top].worth)
            index = top;
        else if (chromo->worth == pool->data[mid].worth)
            index = mid;
        else if (chromo->worth == pool->data[bot].worth)
            index = bot;
        else if (bot - top <= 1)
            index = bot;
        // Narrow search range
        else if (chromo->worth < pool->data[mid].worth)
        {
            bot = mid;
            mid = top + ((bot - top) / 2);
        }
        else
        {
            top = mid;
            mid = top + ((bot - top) / 2);
        }
    }

    // Copy new chromosome to end of pool (replacing worst)
    geqo_copy(root, &pool->data[pool->size - 1], chromo, pool->string_length);

    // Shift chromosomes to make room at insertion point
    swap_chromo.string = pool->data[pool->size - 1].string;
    swap_chromo.worth = pool->data[pool->size - 1].worth;

    for (i = index; i < pool->size; i++)
    {
        tmp_chromo.string = pool->data[i].string;
        tmp_chromo.worth = pool->data[i].worth;

        pool->data[i].string = swap_chromo.string;
        pool->data[i].worth = swap_chromo.worth;

        swap_chromo.string = tmp_chromo.string;
        swap_chromo.worth = tmp_chromo.worth;
    }
}
```