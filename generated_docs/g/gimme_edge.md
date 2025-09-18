# gimme_edge

## Location
src/backend/optimizer/geqo/geqo_erx.c: 154 - 195

## Overview
Registers an edge from one city to another in the edge table for the ERX crossover algorithm, tracking both new and shared edges.

## Definition


## Detailed Description
This static function registers a directed edge from city1 to city2 in the input edge table used by the ERX crossover algorithm. It makes no assumptions about directionality, so calling routines must call gimme_edge twice to create bidirectional edges. The function first checks if the edge already exists in the table. If found, it marks the edge as shared by setting it to a negative value (indicating common edges between parent tours). If the edge doesn't exist, it adds the new edge and increments the total and unused edge counters. The function returns 1 for newly added edges and 0 for existing edges.

## Parameters / Member Variables
- : PlannerInfo pointer containing planning context information (not actively used)
- : Source city/gene for the edge
- : Destination city/gene for the edge
- : Pointer to the Edge table structure where the edge will be registered

## Dependencies
- Functions called/Symbols referenced:
  - Edge (edge table data structure)
  - Gene (genetic algorithm gene data type)  
  - abs (absolute value function for checking shared edges)
- Called from (representative examples):
  - gimme_edge_table (called multiple times to build complete edge table)

## Notes and Other Information
- Function is declared static, making it internal to the geqo_erx.c file
- Shared edges between parent tours are marked with negative values
- Supports both unidirectional and bidirectional edges depending on calling pattern
- Maintains counters for total_edges and unused_edges in the edge table
- Returns 1 for new edges, 0 for existing edges to help track edge diversity
- Part of the ERX algorithm's edge detection and sharing mechanism