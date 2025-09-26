# gimme_gene

## Location
[src/backend/optimizer/geqo/geqo_erx.c:282-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_erx.c#L282-L371)

## Overview
The gimme_gene function selects the next gene to be added to the tour during the ERX (Edge Recombination Crossover) algorithm, giving priority to shared edges and genes with fewer remaining connections.

## Definition
```c
static Gene
gimme_gene(PlannerInfo *root, Edge edge, Edge *edge_table)
```

## Detailed Description
This function implements the gene selection strategy for the ERX crossover operator in PostgreSQL's genetic query optimizer. It analyzes the available candidate genes in the current edge's edge list and selects the most suitable one based on a priority system:

1. **Shared edges priority**: Genes with negative values in the edge list represent shared edges (edges that exist in both parent chromosomes) and are given the highest priority
2. **Minimum edge count priority**: Among non-shared edges, preference is given to genes that have the fewest remaining unused edges
3. **Random selection**: When multiple genes have the same minimum edge count, one is chosen randomly to maintain genetic diversity

The selection strategy helps maintain good connectivity in the resulting tour while preventing the algorithm from getting stuck in local optima through random selection among equally good candidates.

## Parameters / Member Variables
- `root`: PlannerInfo pointer providing access to planner context and random number generation
- `edge`: The current edge structure containing the list of candidate genes to choose from
- `edge_table`: Array of Edge structures representing the complete edge table with connectivity information

## Dependencies
- Functions called/Symbols referenced:
  - [Edge](../E/Edge.md) (type)
  - [Gene](../G/Gene.md) (type)
  - [geqo_randint](geqo_randint.md) (random number generation function)
- Called from (representative examples):
  - [gimme_tour](gimme_tour.md)

## Notes and Other Information
- This is a static function, only accessible within the geqo_erx.c file
- Negative values in edge lists indicate shared edges between parent chromosomes
- The function uses a two-pass algorithm: first pass to find minimum edge counts, second pass to make the final selection
- Includes error checking for cases where no valid gene can be found
- The random selection mechanism helps maintain population diversity in the genetic algorithm
- Part of the ERX crossover operator implementation which is known for preserving edge information from parent solutions