# gimme_edge

## Location
[src/backend/optimizer/geqo/geqo_erx.c:154-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L154-L195)

## Overview
Registers an edge from one city to another in the edge table for the ERX crossover algorithm, tracking both new and shared edges.

## Definition

```c
static int
gimme_edge(PlannerInfo *root, Gene gene1, Gene gene2, Edge *edge_table)
```
## Detailed Description
This static function registers a directed edge from city1 to city2 in the input edge table used by the ERX crossover algorithm. It makes no assumptions about directionality, so calling routines must call gimme_edge twice to create bidirectional edges. The function first checks if the edge already exists in the table. If found, it marks the edge as shared by setting it to a negative value (indicating common edges between parent tours). If the edge doesn't exist, it adds the new edge and increments the total and unused edge counters. The function returns 1 for newly added edges and 0 for existing edges.

## Parameters / Member Variables
- : PlannerInfo pointer containing planning context information (not actively used)
- : Source city/gene for the edge
- : Destination city/gene for the edge
- : Pointer to the Edge table structure where the edge will be registered

## Dependencies
- Functions called/Symbols referenced:
  - [Edge](../E/Edge.md) (edge table data structure)
  - [Gene](../G/Gene.md) (genetic algorithm gene data type)  
  - abs (absolute value function for checking shared edges)
- Called from (representative examples):
  - [gimme_edge_table](gimme_edge_table.md) (called multiple times to build complete edge table)

## Notes and Other Information
- Function is declared static, making it internal to the geqo_erx.c file
- Shared edges between parent tours are marked with negative values
- Supports both unidirectional and bidirectional edges depending on calling pattern
- Maintains counters for total_edges and unused_edges in the edge table
- Returns 1 for new edges, 0 for existing edges to help track edge diversity
- Part of the ERX algorithm's edge detection and sharing mechanism

## Simplified Source

```c
static int gimme_edge(PlannerInfo *root, Gene gene1, Gene gene2, Edge *edge_table) {
    int i;
    int edges;
    int city1 = (int) gene1;
    int city2 = (int) gene2;

    // Check if edge city1->city2 already exists
    edges = edge_table[city1].total_edges;

    for (i = 0; i < edges; i++) {
        if ((Gene) abs(edge_table[city1].edge_list[i]) == city2) {
            // Mark shared edges as negative
            edge_table[city1].edge_list[i] = 0 - city2;
            return 0; // Edge already existed
        }
    }

    // Add new edge city1->city2
    edge_table[city1].edge_list[edges] = city2;

    // Update edge counters
    edge_table[city1].total_edges++;
    edge_table[city1].unused_edges++;

    return 1; // New edge added
}
```