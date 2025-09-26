# geqo_eval

## Location
[src/backend/optimizer/geqo/geqo_eval.c:57-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_eval.c#L57-L162)

## Overview
Evaluates the fitness of a given gene tour in the GEQO (Genetic Query Optimizer) algorithm by constructing a join tree and returning its total cost.

## Definition

```c
struct HTAB *savehash;
```
## Detailed Description
The geqo_eval function is a critical component of PostgreSQL's genetic query optimizer that evaluates the fitness of individual chromosomes (gene tours) in the genetic population. It takes a gene tour representing a join order and constructs an actual query plan to determine the cost, which serves as the fitness value for the genetic algorithm.

The function operates by creating a temporary memory context to avoid memory leaks during repeated evaluations, then calling gimme_tree() to construct the optimal join tree for the given gene sequence. If a valid join order can be extracted, it returns the total cost of the cheapest path; otherwise, it returns DBL_MAX to indicate an invalid solution.

The function carefully manages the planner's join_rel_list and join_rel_hash to ensure they are restored to their original state after evaluation, preventing interference between different evaluations in the genetic algorithm.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and metadata
- : Array of Gene values representing the proposed join order for evaluation
- : Integer specifying the number of genes (relations) in the tour array

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context management)
  - [gimme_tree](gimme_tree.md) (constructs join tree from gene tour)
  - [list_truncate](../l/list_truncate.md) (restores join_rel_list state)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (cleanup)
  - [Gene](../G/Gene.md), Cost, HTAB (type definitions)
  - ALLOCSET_DEFAULT_SIZES (memory allocation constant)

- Called from (representative examples):
  - [geqo](geqo.md) (main genetic algorithm driver)
  - [random_init_pool](../r/random_init_pool.md) (population initialization)

## Notes and Other Information
- Returns DBL_MAX for invalid join orders that cannot be constructed
- Uses temporary memory contexts to prevent memory leaks during repeated evaluations
- Does not currently support optimization for partial result retrieval or parameterized paths
- Assumes new entries to join_rel_list are appended at the end for proper restoration
- Critical for fitness evaluation in the genetic query optimization process