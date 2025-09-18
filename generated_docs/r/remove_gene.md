# remove_gene

## Location
src/backend/optimizer/geqo/geqo_erx.c: 240 - 281

## Overview
The remove_gene function removes a specified gene from the edge table structure, which is part of the ERX (Edge Recombination Crossover) algorithm used in the GEQO (Genetic Query Optimizer).

## Definition


## Detailed Description
This function is responsible for removing a specific gene from the edge table data structure used in the genetic algorithm's edge recombination crossover operation. When a gene is selected for inclusion in the offspring tour, it must be removed from all edge lists to prevent it from being selected again. The function iterates through all genes that have edges to the input gene (stored in the edge's edge_list) and removes the input gene from their respective edge lists.

The removal process involves finding the gene in each edge list and replacing it with the last element in the list, then decrementing the unused_edges counter. This maintains the compactness of the edge lists while preserving the integrity of the edge table structure.

## Parameters / Member Variables
- : PlannerInfo pointer providing access to planner context (not directly used in this function)
- : The gene to be removed from the edge table
- : The edge structure containing the list of genes that have edges to the input gene
- : Array of Edge structures representing the complete edge table

## Dependencies
- Functions called/Symbols referenced:
  - Edge (type)
  - Gene (type)
- Called from (representative examples):
  - gimme_tour

## Notes and Other Information
- This is a static function, only accessible within the geqo_erx.c file
- The function uses absolute values when accessing edge lists, suggesting that negative values may have special meaning in the edge representation
- The removal algorithm maintains O(1) complexity by swapping with the last element rather than shifting all subsequent elements
- Part of the ERX crossover operator implementation in PostgreSQL's genetic query optimizer