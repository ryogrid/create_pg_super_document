# alloc_pool

## Location
[src/backend/optimizer/geqo/geqo_pool.c:42-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L42-L68)

## Overview
Allocates memory for a genetic algorithm pool structure used in PostgreSQL's GEQO (Genetic Query Optimizer) for managing a collection of chromosomes representing query execution plans.

## Definition

```c
Pool *
alloc_pool(PlannerInfo *root, int pool_size, int string_length)
```
## Detailed Description
The  function creates and initializes a memory pool for the genetic algorithm optimizer. It allocates memory for a Pool structure that contains an array of chromosomes, where each chromosome represents a potential query execution plan. The function performs three main memory allocations:

1. Allocates memory for the main Pool structure
2. Allocates memory for an array of Chromosome structures based on pool_size
3. For each chromosome, allocates memory for its gene string based on string_length

This function is fundamental to the GEQO system as it sets up the data structures needed to store and manipulate multiple candidate query execution plans during genetic optimization.

## Parameters / Member Variables
- : PlannerInfo pointer providing planning context (though not directly used in this function)
- : Integer specifying the number of chromosomes to allocate in the pool
- : Integer specifying the length of the gene string for each chromosome

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - Pool (struct type for genetic algorithm pool)
  - Chromosome (struct type for individual solutions)
  - Gene (type for genetic algorithm genes)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO optimization function)

## Notes and Other Information
- Uses PostgreSQL's palloc() memory allocation function which provides automatic memory management within the current memory context
- The allocated pool must be properly freed using free_pool() to avoid memory leaks
- Each chromosome's gene string is allocated with (string_length + 1) to accommodate string termination
- This function is part of PostgreSQL's genetic query optimizer which is used for complex join ordering problems involving many tables