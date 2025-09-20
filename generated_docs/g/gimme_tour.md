# gimme_tour

## Location
[src/backend/optimizer/geqo/geqo_erx.c:196-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L196-L239)

## Overview
Creates a new tour using edges from the edge table, prioritizing shared edges between parent tours in the ERX crossover operation.

## Definition

```c
int
gimme_tour(PlannerInfo *root, Edge *edge_table, Gene *new_gene, int num_gene)
```
## Detailed Description
This function creates a new tour (offspring) by constructing a path through all cities using the edge information stored in the edge table. The algorithm starts with a randomly selected city and iteratively builds the tour by following available edges. Priority is given to shared edges (marked as negative values) that exist in both parent tours. For each city added to the tour, the function removes that city from all edge lists to prevent revisiting. When no valid edges are available from the current city, it handles the fault condition using edge_failure(). The function returns the total number of edge failures encountered during tour construction.

## Parameters / Member Variables
- : PlannerInfo pointer containing planning context and random number generation state
- : Pointer to the Edge table containing edge information from parent tours
- : Pointer to the Gene array where the new tour will be stored
- : Integer specifying the number of cities/genes in the tour

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_randint](geqo_randint.md) (random number generation for initial city selection)
  - [remove_gene](../r/remove_gene.md) (removes city from edge lists)
  - [gimme_gene](gimme_gene.md) (selects next city from available edges)
  - [edge_failure](../e/edge_failure.md) (handles cases when no valid edges available)
  - Edge (edge table data structure)
  - Gene (genetic algorithm gene data type)
- Called from (representative examples):
  - [geqo](geqo.md) (main genetic algorithm function during crossover)

## Notes and Other Information
- Implements the tour construction phase of ERX crossover algorithm
- Prioritizes shared edges to preserve common patterns from both parents
- Handles edge failure cases gracefully when tour construction gets stuck
- Marks incorporated cities with unused_edges = -1 to track progress
- Returns edge failure count as quality metric for the crossover operation
- Random starting city selection ensures diversity in offspring generation
- Part of the ERX (Edge Recombination Crossover) implementation in GEQO