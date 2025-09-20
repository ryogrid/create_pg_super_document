# geqo_copy

## Location
[src/backend/optimizer/geqo/geqo_copy.c:45-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_copy.c#L45-L54)

## Overview
Copies the contents of one chromosome to another in the GEQO (Genetic Query Optimizer) algorithm implementation.

## Definition

```c
void
geqo_copy(PlannerInfo *root, Chromosome *chromo1, Chromosome *chromo2,
		  int string_length)
```
## Detailed Description
The  function performs a complete deep copy of chromosome data from one chromosome structure to another. This function is a fundamental utility in PostgreSQL's genetic algorithm-based query optimizer (GEQO). It copies both the gene sequence (represented as an array of integers) and the fitness value (worth) from the source chromosome to the destination chromosome. The function operates by iterating through the gene string and copying each element individually, then copying the fitness score.

## Parameters / Member Variables
- : PlannerInfo pointer containing planner context (not used in this function but follows GEQO interface conventions)
- : Destination chromosome that will receive the copied data
- : Source chromosome from which data will be copied
- : Length of the gene string to be copied (number of elements in the chromosome array)

## Dependencies
- Functions called/Symbols referenced:
  - Chromosome (struct type)
- Called from (representative examples):
  - [spread_chromo](../s/spread_chromo.md)
  - [geqo_selection](geqo_selection.md)

## Notes and Other Information
- This is a utility function that performs a straightforward array copy operation plus fitness value transfer
- The function assumes that both chromosomes have been properly allocated and that the destination chromosome has sufficient space for the specified string_length
- Part of the GEQO module which implements a genetic algorithm approach for query optimization when dealing with large numbers of joins
- The function follows the GEQO naming convention with the 'geqo_' prefix
- Located in src/backend/optimizer/geqo/geqo_copy.c:45-54