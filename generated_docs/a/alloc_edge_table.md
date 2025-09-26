# alloc_edge_table

## Location
[src/backend/optimizer/geqo/geqo_erx.c:56-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L56-L75)

## Overview
Allocates memory for an edge table used in the GEQO (Genetic Query Optimizer) ERX (Edge Recombination Crossover) algorithm.

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
This function allocates memory for an edge table that is used in the ERX crossover operation within PostgreSQL's genetic algorithm-based query optimizer. The function allocates space for  Edge structures, where the extra location allows nodes numbered 1 through n to be indexed directly, with index 0 remaining unused. This indexing scheme is common in genetic algorithms to simplify node referencing.

## Parameters / Member Variables
- : PlannerInfo pointer containing planning context information
- : Integer specifying the number of genes (relations) in the genetic algorithm, determining the size of the edge table

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation function)
  - [Edge](../E/Edge.md) (data structure type)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main genetic algorithm function)

## Notes and Other Information
- The function allocates one extra Edge structure to enable 1-based indexing
- Uses PostgreSQL's palloc memory allocation which provides automatic cleanup
- Part of the ERX crossover implementation in the genetic query optimizer
- The edge table stores connectivity information between nodes in the genetic algorithm's tour representation