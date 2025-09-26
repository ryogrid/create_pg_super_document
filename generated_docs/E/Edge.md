# Edge

## Location
[src/include/optimizer/geqo_recombination.h:35-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/geqo_recombination.h#L35-L40)

## Overview
The Edge struct is a core data structure used in PostgreSQL's Genetic Query Optimizer (GEQO) for edge recombination crossover (ERX) operations during genetic algorithm-based query optimization.

## Definition

```c
typedef struct Edge
{
	Gene		edge_list[4];	/* list of edges */
	int			total_edges;
	int			unused_edges;
} Edge;
```
## Detailed Description
The Edge structure is used in the GEQO edge recombination crossover algorithm to represent graph edges between genes (which typically correspond to relations in query planning). Each Edge maintains a list of up to 4 connected genes and tracks both the total number of edges and the number of unused edges. This structure is fundamental to the ERX genetic algorithm which attempts to preserve good building blocks (edges) from parent solutions when creating offspring solutions in the genetic optimization process.

The edge recombination crossover is designed to preserve adjacency information from parent tours, making it particularly suitable for problems where the ordering and adjacency relationships are important, such as in query join ordering optimization.

## Parameters / Member Variables
- `edge_list[4]`: Array of Gene values representing up to 4 edges connected to this node in the adjacency graph
- `total_edges`: Integer count of the total number of edges associated with this node
- `unused_edges`: Integer count of edges that have not yet been used during the recombination process
## Dependencies
- Functions called/Symbols referenced:
  - [Gene](../G/Gene.md) (typedef for int, representing a relation identifier)
- Called from (representative examples):
  - [alloc_edge_table](../a/alloc_edge_table.md) (allocates arrays of Edge structures)
  - [gimme_edge_table](../g/gimme_edge_table.md) (creates and initializes edge table)
  - [gimme_edge](../g/gimme_edge.md) (retrieves edge information)
  - [gimme_tour](../g/gimme_tour.md) (constructs tour using edge information)
  - [remove_gene](../r/remove_gene.md) (removes genes from edge lists)
  - [gimme_gene](../g/gimme_gene.md) (selects genes based on edge information)
  - [print_edge_table](../p/print_edge_table.md) (debugging function to print edge table)

## Notes and Other Information
- The Edge structure is specifically designed for the ERX algorithm implementation in GEQO
- The fixed size of 4 for the edge_list array reflects the maximum degree constraint typically used in ERX algorithms
- This structure is part of PostgreSQL's genetic query optimizer, which is used for complex queries with many relations where exhaustive search would be too expensive
- The ERX algorithm is particularly effective at preserving good adjacency relationships from parent solutions while allowing for beneficial mutations in the genetic optimization process