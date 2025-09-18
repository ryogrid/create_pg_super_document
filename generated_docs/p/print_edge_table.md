# print_edge_table

## Location
src/backend/optimizer/geqo/geqo_misc.c: 112 - 132

## Overview
The print_edge_table function outputs a formatted representation of an edge table data structure used in GEQO's edge recombination crossover operations.

## Definition


## Detailed Description
This function prints the contents of an edge table, which is a key data structure used in genetic algorithm edge recombination crossover. The edge table stores adjacency information for each gene (city/node), showing which other genes are connected to it in the parent chromosomes. For each gene from 1 to num_gene, it displays the gene number followed by its list of adjacent genes (edges). This information is crucial for debugging edge recombination operations in the GEQO system, where maintaining edge relationships from parent chromosomes is important for preserving good partial solutions.

## Parameters / Member Variables
- : File pointer where the output will be written
- : Array of Edge structures containing adjacency information for each gene
- : The total number of genes (size of the edge table, typically number of relations)

## Dependencies
- Functions called/Symbols referenced:
  - Edge (structure type)
  - fprintf (for formatted output)
  - fflush (to ensure output is written)
- Called from (representative examples):
  - GEQO_MISC_H (header declaration)

## Notes and Other Information
- This is a public function (non-static) available for use by other GEQO modules
- The edge table is 1-indexed (starts from gene 1, not 0)
- Each edge entry contains unused_edges count and an edge_list array
- Output format includes a header 'EDGE TABLE' followed by each gene's adjacency list
- Each line shows: 'gene_number : adjacent_gene1 adjacent_gene2 ...'
- Primarily used for debugging edge recombination crossover operations
- Part of the GEQO (Genetic Query Optimizer) debugging infrastructure
- Automatically flushes output to ensure immediate visibility