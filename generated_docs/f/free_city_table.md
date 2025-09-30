# free_city_table

## Location
[src/backend/optimizer/geqo/geqo_recombination.c:87-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_recombination.c#L87-L92)

## Overview
Deallocates memory for a city table previously allocated by alloc_city_table in PostgreSQL's genetic query optimizer (GEQO).

## Definition
```c
void free_city_table(PlannerInfo *root, City *city_table)
```

## Detailed Description
This function is the counterpart to alloc_city_table and is responsible for cleaning up the memory allocated for the City table used in GEQO operations. It simply calls pfree() to deallocate the memory pointed to by city_table. This function is part of the memory management discipline in PostgreSQL's genetic query optimizer, ensuring that temporary data structures used during join optimization are properly cleaned up.

The function follows PostgreSQL's memory management conventions by using pfree() rather than standard free(), allowing the memory to be reclaimed within PostgreSQL's memory context system.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing the query planning context (unused in current implementation but maintained for consistency with alloc_city_table)
- `city_table`: Pointer to the City table to be deallocated, previously allocated by alloc_city_table

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL's memory deallocation function)
  - [City](../C/City.md) (type representing relation information in GEQO context)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO function, called multiple times during cleanup phases)

## Notes and Other Information
- Part of PostgreSQL's Genetic Query Optimizer (GEQO) memory management
- Must be called to avoid memory leaks after GEQO operations complete
- Counterpart function to alloc_city_table - they should always be used as a pair
- Uses PostgreSQL's pfree() for proper memory context management
- Simple wrapper around pfree() but maintains consistent interface with allocation function

## Simplified Source

```c
void
free_city_table(PlannerInfo *root, City *city_table)
{
    // Deallocate city table memory
    pfree(city_table);
}
```