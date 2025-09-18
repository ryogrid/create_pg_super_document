# edge_failure

## Location
src/backend/optimizer/geqo/geqo_erx.c: 372 - 470

## Overview
The edge_failure function handles the situation when no suitable gene can be selected through normal ERX algorithm rules, providing fallback strategies to continue tour construction.

## Definition
```c
static Gene
edge_failure(PlannerInfo *root, Gene *gene, int index, Edge *edge_table, int num_gene)
```

## Detailed Description
This function serves as a fallback mechanism in the ERX crossover algorithm when the normal gene selection process fails (typically when the current gene has no remaining edges in its edge list). It implements a three-tier priority system for selecting the next gene to continue tour construction:

1. **Priority 1 - Genes with 4 total edges**: First preference is given to genes that originally had 4 edges (maximum connectivity), as these are likely to be central nodes that help maintain tour quality
2. **Priority 2 - Any remaining genes**: If no 4-edge genes are available, randomly select from any genes that still have unused edges
3. **Priority 3 - Last resort**: When the edge table appears empty (usually at the very end of tour construction), find any gene that hasn't been completely processed

Each selection within a priority tier is made randomly to maintain genetic diversity and prevent deterministic behavior that could lead to premature convergence.

## Parameters / Member Variables
- `root`: PlannerInfo pointer providing access to planner context and random number generation
- `gene`: Array representing the current partial tour being constructed
- `index`: Current position in the tour (index of the gene that failed to find suitable edges)
- `edge_table`: Array of Edge structures representing the complete edge table with connectivity information
- `num_gene`: Total number of genes (cities/relations) in the problem

## Dependencies
- Functions called/Symbols referenced:
  - Edge (type)
  - Gene (type)
  - [geqo_randint](../g/geqo_randint.md) (random number generation function)
- Called from (representative examples):
  - [gimme_tour](../g/gimme_tour.md)

## Notes and Other Information
- This is a static function, only accessible within the geqo_erx.c file
- The function uses `unused_edges != -1` to identify available genes (-1 indicates a gene has been fully processed)
- Genes with 4 total edges represent high-connectivity nodes that are prioritized for tour quality
- The three-tier fallback system ensures the algorithm can always make progress, even in degenerate cases
- Extensive logging helps with debugging edge cases in the genetic algorithm
- This mechanism is crucial for the robustness of the ERX crossover operator, ensuring tours can always be completed even when the edge recombination strategy breaks down