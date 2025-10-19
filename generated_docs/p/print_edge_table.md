# print_edge_table

## Location
[src/backend/optimizer/geqo/geqo_misc.c:112-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_misc.c#L112-L132)

## Overview
The print_edge_table function outputs a formatted representation of an edge table data structure used in GEQO's edge recombination crossover operations.

## Definition

```c
void
print_edge_table(FILE *fp, Edge *edge_table, int num_gene)
```
## Detailed Description
This function prints the contents of an edge table, which is a key data structure used in genetic algorithm edge recombination crossover. The edge table stores adjacency information for each gene (city/node), showing which other genes are connected to it in the parent chromosomes. For each gene from 1 to num_gene, it displays the gene number followed by its list of adjacent genes (edges). This information is crucial for debugging edge recombination operations in the GEQO system, where maintaining edge relationships from parent chromosomes is important for preserving good partial solutions.

## Parameters / Member Variables
- `*fp`: File pointer where the output will be written
- `*edge_table`: Array of Edge structures containing adjacency information for each gene
- `num_gene`: The total number of genes (size of the edge table, typically number of relations)
## Dependencies
- Functions called/Symbols referenced:
  - [Edge](../E/Edge.md) (structure type)
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

## Simplified Source

```c
void
print_edge_table(FILE *fp, Edge *edge_table, int num_gene)
{
    // Print header for the edge table
    fprintf(fp, "\nEDGE TABLE\n");

    // Print each gene and its adjacent edges (1-indexed)
    for (int i = 1; i <= num_gene; i++) {
        // Print gene number
        fprintf(fp, "%d :", i);

        // Print all adjacent genes for this gene
        for (int j = 0; j < edge_table[i].unused_edges; j++)
            fprintf(fp, " %d", edge_table[i].edge_list[j]);

        // End line for this gene
        fprintf(fp, "\n");
    }

    // Add spacing and ensure output is written
    fprintf(fp, "\n");
    fflush(fp);
}
```

**Key Simplifications:**
- Combined variable declarations with loop initialization
- Added descriptive comments for each section
- Clarified the 1-indexed nature of the gene numbering
- Preserved the essential edge table printing logic
- Maintained the debugging output format with proper spacing
- Kept the fflush to ensure immediate output visibility