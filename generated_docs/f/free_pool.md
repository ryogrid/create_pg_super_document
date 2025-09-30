# free_pool

## Location
[src/backend/optimizer/geqo/geqo_pool.c:69-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L69-L90)

## Overview
Deallocates memory for a genetic algorithm pool structure used in PostgreSQL's GEQO, properly freeing all allocated memory including chromosomes and their gene strings.

## Definition
```c
void free_pool(PlannerInfo *root, Pool *pool)
```

## Detailed Description
The `free_pool` function is the complement to `alloc_pool`, responsible for properly deallocating all memory associated with a genetic algorithm pool. It performs memory deallocation in the reverse order of allocation to ensure proper cleanup:

1. First frees each chromosome's gene string array
2. Then frees the array of chromosome structures
3. Finally frees the main Pool structure itself

This function is critical for preventing memory leaks in the GEQO system, as genetic algorithm pools can contain substantial amounts of memory depending on the pool size and chromosome length.

## Parameters / Member Variables
- `root`: PlannerInfo pointer providing planning context (though not directly used in this function)
- `pool`: Pointer to the Pool structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [Pool](../P/Pool.md) (struct type for genetic algorithm pool)
  - [Chromosome](../C/Chromosome.md) (struct type for individual solutions)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO optimization function for cleanup)

## Notes and Other Information
- Must be called to free memory allocated by alloc_pool() to prevent memory leaks
- Uses PostgreSQL's pfree() function which is the counterpart to palloc()
- Frees memory in reverse allocation order: gene strings → chromosome array → pool structure
- The function assumes the pool structure is valid and properly initialized
- Part of PostgreSQL's genetic query optimizer memory management system

## Simplified Source

```c
void free_pool(PlannerInfo *root, Pool *pool) {
    Chromosome *chromo = (Chromosome *) pool->data;

    // Free each chromosome's gene string
    for (int i = 0; i < pool->size; i++) {
        pfree(chromo[i].string);
    }

    // Free chromosome array
    pfree(pool->data);

    // Free pool structure
    pfree(pool);
}
```