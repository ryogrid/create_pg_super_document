# gimme_edge_table

## Location
[src/backend/optimizer/geqo/geqo_erx.c:95-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L95-L153)

## Overview
Constructs an edge table representing the set of explicit edges between points in two input genetic algorithm tours, supporting the ERX crossover operation.

## Definition

```c
float
gimme_edge_table(PlannerInfo *root, Gene *tour1, Gene *tour2,
				 int num_gene, Edge *edge_table)
```
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
  - [Gene](../G/Gene.md) (genetic algorithm gene data type)
  - [Edge](../E/Edge.md) (edge table data structure)
- Called from (representative examples):
  - [geqo](geqo.md) (main genetic algorithm function during crossover operations)

## Notes and Other Information
- Assumes tours are circular (last city connects back to first city)
- Processes edges as bidirectional (each edge added in both directions)
- Clears existing edge table data before filling with new information
- The return value indicates tour diversity: 2.0 means tours are identical, 4.0 means completely different
- Used in the ERX (Edge Recombination Crossover) algorithm to identify common edges between parent tours
- [Edge](../E/Edge.md) processing involves calling gimme_edge twice for each connection to handle bidirectionality

## Simplified Source

```c
float gimme_edge_table(PlannerInfo *root, Gene *tour1, Gene *tour2,
                      int num_gene, Edge *edge_table) {
    int i, index1, index2;
    int edge_total = 0;

    // Clear edge table data
    for (i = 1; i <= num_gene; i++) {
        edge_table[i].total_edges = 0;
        edge_table[i].unused_edges = 0;
    }

    // Process both tours to build edge table
    for (index1 = 0; index1 < num_gene; index1++) {
        // Circular tour: map last index back to first
        index2 = (index1 + 1) % num_gene;

        // Add bidirectional edges from both tours
        edge_total += gimme_edge(root, tour1[index1], tour1[index2], edge_table);
        gimme_edge(root, tour1[index2], tour1[index1], edge_table);

        edge_total += gimme_edge(root, tour2[index1], tour2[index2], edge_table);
        gimme_edge(root, tour2[index2], tour2[index1], edge_table);
    }

    // Return average edges per gene (2.0-4.0 range)
    return ((float) (edge_total * 2) / (float) num_gene);
}
```