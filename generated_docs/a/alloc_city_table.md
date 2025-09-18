# alloc_city_table

## Location
src/backend/optimizer/geqo/geqo_recombination.c: 69 - 86

## Overview
Allocates memory for a city table used in PostgreSQL's genetic query optimizer (GEQO) to represent relations in join optimization.

## Definition
```c
City *alloc_city_table(PlannerInfo *root, int num_gene)
```

## Detailed Description
This function allocates memory for a City table that maps relation indices to their corresponding RelOptInfo structures in the genetic query optimizer. The function allocates one extra location beyond the required number of genes so that nodes can be indexed directly from 1 to n, with index 0 remaining unused. This indexing scheme aligns with the natural numbering used in the traveling salesman problem formulation within GEQO.

The allocated table serves as a lookup structure that connects the abstract gene representation (integers 1..n) used in genetic algorithm operations to the actual database relations being joined.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing the query planning context (unused in current implementation but maintained for consistency)
- `num_gene`: Number of genes (relations) in the optimization problem, determines the table size

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL's memory allocation function)
  - City (type representing relation information in GEQO context)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO function, called multiple times for different algorithm phases)

## Notes and Other Information
- Part of PostgreSQL's Genetic Query Optimizer (GEQO) memory management
- Allocates (num_gene + 1) entries to allow 1-based indexing
- Memory should be freed using corresponding free_city_table function
- The City type typically contains RelOptInfo pointers and related metadata
- Critical for establishing the mapping between genetic algorithm genes and actual database relations