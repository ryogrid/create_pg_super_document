# free_edge_table

## Location
[src/backend/optimizer/geqo/geqo_erx.c:76-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L76-L94)

## Overview
Deallocates memory for an edge table used in the GEQO ERX algorithm, providing proper cleanup of allocated resources.

## Definition

```c
structure which represents the set of explicit
 *	 edges between points in the (2) input genes
 *
 *	 assumes circular tours and bidirectional edges
 *
 *	 gimme_edge() will set "shared" edges to negative values
 *
 *	 returns average number edges/city in range 2.0 - 4.0
 *	 where 2.0=homogeneous;
```
## Detailed Description
This function deallocates the memory previously allocated for an edge table in the GEQO ERX crossover algorithm. It serves as the cleanup counterpart to alloc_edge_table, ensuring proper memory management within PostgreSQL's genetic query optimizer. The function uses PostgreSQL's pfree function to release the memory allocated by palloc.

## Parameters / Member Variables
- : PlannerInfo pointer containing planning context information (not actively used in deallocation)
- : Pointer to the Edge table structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation function)
  - Edge (data structure type)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main genetic algorithm function for cleanup)

## Notes and Other Information
- Should be called to free memory allocated by alloc_edge_table
- Uses PostgreSQL's pfree which is the counterpart to palloc
- Part of proper resource management in the genetic query optimizer
- The root parameter is included for consistency with other GEQO functions but is not used in the deallocation process