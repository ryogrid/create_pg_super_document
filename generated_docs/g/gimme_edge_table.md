# gimme_edge_table

## Location
[src/backend/optimizer/geqo/geqo_erx.c:95-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L95-L153)

## Overview
Constructs an edge table representing the set of explicit edges between points in two input genetic algorithm tours, supporting the ERX crossover operation.

## Definition


## Detailed Description
This function fills an edge table data structure that represents the set of explicit edges between points in two input genetic algorithm tours. It assumes circular tours and bidirectional edges, meaning each connection between adjacent cities is represented in both directions. The function processes both input tours, extracting their edge information and populating the edge table. Shared edges between the two tours are marked by gimme_edge() with negative values. The function returns the average number of edges per city, which ranges from 2.0 (homogeneous tours) to 4.0 (diverse tours), providing a measure of tour diversity.

## Parameters / Member Variables
- : PlannerInfo pointer containing planning context information
- : Pointer to the first genetic algorithm tour (Gene array)
- : Pointer to the second genetic algorithm tour (Gene array)  
- : Integer specifying the number of genes (cities/relations) in the tours
- : Pointer to the Edge table structure to be filled with edge information

## Dependencies
- Functions called/Symbols referenced:
  - [gimme_edge](gimme_edge.md) (called to add individual edges to the table)
  - Gene (genetic algorithm gene data type)
  - Edge (edge table data structure)
- Called from (representative examples):
  - [geqo](geqo.md) (main genetic algorithm function during crossover operations)

## Notes and Other Information
- Assumes tours are circular (last city connects back to first city)
- Processes edges as bidirectional (each edge added in both directions)
- Clears existing edge table data before filling with new information
- The return value indicates tour diversity: 2.0 means tours are identical, 4.0 means completely different
- Used in the ERX (Edge Recombination Crossover) algorithm to identify common edges between parent tours
- Edge processing involves calling gimme_edge twice for each connection to handle bidirectionality