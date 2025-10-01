# edge_failure

## Location
[src/backend/optimizer/geqo/geqo_erx.c:372-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L372-L470)

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
  - [Edge](../E/Edge.md) (type)
  - [Gene](../G/Gene.md) (type)
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

## Simplified Source

```c
static Gene edge_failure(PlannerInfo *root, Gene *gene, int index, Edge *edge_table, int num_gene) {
    int i;
    Gene fail_gene = gene[index];
    int remaining_edges = 0;
    int four_count = 0;
    int rand_decision;

    // Count remaining edges and genes with 4 total edges
    for (i = 1; i <= num_gene; i++) {
        if ((edge_table[i].unused_edges != -1) && (i != (int) fail_gene)) {
            remaining_edges++;
            if (edge_table[i].total_edges == 4)
                four_count++;
        }
    }

    // Priority 1: Random selection from genes with 4 total edges
    if (four_count != 0) {
        rand_decision = geqo_randint(root, four_count - 1, 0);
        for (i = 1; i <= num_gene; i++) {
            if ((Gene) i != fail_gene &&
                edge_table[i].unused_edges != -1 &&
                edge_table[i].total_edges == 4) {
                four_count--;
                if (rand_decision == four_count)
                    return (Gene) i;
            }
        }
    }
    // Priority 2: Random selection from any remaining genes
    else if (remaining_edges != 0) {
        rand_decision = geqo_randint(root, remaining_edges - 1, 0);
        for (i = 1; i <= num_gene; i++) {
            if ((Gene) i != fail_gene &&
                edge_table[i].unused_edges != -1) {
                remaining_edges--;
                if (rand_decision == remaining_edges)
                    return i;
            }
        }
    }
    // Priority 3: Last resort - find any unused point
    else {
        for (i = 1; i <= num_gene; i++)
            if (edge_table[i].unused_edges >= 0)
                return (Gene) i;
    }

    // Should never reach here
    elog(ERROR, "no edge found");
    return 0;
}
```