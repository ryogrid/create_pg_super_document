# free_chromo

## Location
[src/backend/optimizer/geqo/geqo_pool.c:176-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L176-L186)

## Overview
The  function deallocates memory for a chromosome structure and its associated gene string space in PostgreSQL's Genetic Query Optimizer (GEQO).

## Definition

```c
void
free_chromo(PlannerInfo *root, Chromosome *chromo)
```
## Detailed Description
This function is the counterpart to , responsible for properly deallocating memory that was previously allocated for a chromosome structure. It performs the deallocation in the correct order: first freeing the gene string array, then freeing the chromosome structure itself. This ensures proper memory management and prevents memory leaks within the GEQO framework.

The function uses PostgreSQL's  memory deallocation function, which is the standard way to free memory that was allocated with . The deallocation order is important to avoid accessing freed memory.

## Parameters / Member Variables
- : PlannerInfo pointer representing the planner context (though not directly used in the current implementation)
- : Pointer to the Chromosome structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL memory deallocator)
  -  (structure type)
- Called from (representative examples):
  -  function in geqo_main.c (multiple locations for cleanup)

## Notes and Other Information
- The function deallocates in the correct order: gene string first, then the chromosome structure
- Memory deallocation is performed using pfree, which is PostgreSQL's standard memory management function
- This is a critical cleanup function for the GEQO genetic algorithm implementation to prevent memory leaks
- The function is declared in geqo.h
- Must be called for every chromosome allocated with alloc_chromo to ensure proper memory management

## Simplified Source

```c
void
free_chromo(PlannerInfo *root, Chromosome *chromo)
{
    // Free gene string first, then chromosome structure
    pfree(chromo->string);
    pfree(chromo);
}
```