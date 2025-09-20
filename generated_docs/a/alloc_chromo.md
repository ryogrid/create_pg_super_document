# alloc_chromo

## Location
[src/backend/optimizer/geqo/geqo_pool.c:162-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L162-L175)

## Overview
The  function allocates memory for a new chromosome structure and its associated gene string space in PostgreSQL's Genetic Query Optimizer (GEQO).

## Definition

```c
Chromosome *
alloc_chromo(PlannerInfo *root, int string_length)
```
## Detailed Description
This function is a memory allocation utility within the GEQO framework that creates a new chromosome data structure. It performs two key memory allocations: one for the Chromosome structure itself and another for the gene string array that will hold the chromosome's genetic information. The function uses PostgreSQL's  memory allocation function, which provides automatic memory management within the current memory context.

The allocated chromosome includes space for  genes, with the extra slot likely used for termination or padding purposes. This is a fundamental building block for the genetic algorithm operations within PostgreSQL's query optimization process.

## Parameters / Member Variables
- : PlannerInfo pointer representing the planner context (though not directly used in the current implementation)
- : Integer specifying the number of genes the chromosome should accommodate

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL memory allocator)
  -  (structure type)
  -  (data type for individual genes)
- Called from (representative examples):
  -  function in geqo_main.c (multiple locations)

## Notes and Other Information
- The function allocates  gene slots, suggesting space for a terminator or buffer
- Memory is allocated using , which means it will be automatically freed when the current memory context is reset
- This is a core utility function for the GEQO genetic algorithm implementation
- The function is declared in 