# px

## Location
[src/backend/optimizer/geqo/geqo_px.c:49-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_px.c#L49-L109)

## Overview
Implements Position Crossover (PX) genetic algorithm operator for GEQO (Genetic Query Optimization) to create offspring solutions by combining elements from two parent solutions.

## Definition

```c
void
px(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene,
   City * city_table)
```
## Detailed Description
The px function implements the Position Crossover operator according to Syswerda's algorithm from "The Genetic Algorithms Handbook" edited by L Davis. This crossover operator is used within PostgreSQL's Genetic Query Optimization (GEQO) system to generate new query execution plans by combining elements from two parent plans.

The algorithm works in two phases:
1. **Random Position Selection**: Randomly selects a subset of positions (between num_gene/3 and 2*num_gene/3) that will be inherited directly from the first parent (tour1)
2. **Sequential Fill**: Fills the remaining positions by sequentially traversing the second parent (tour2), skipping any genes that have already been used

The PX operator ensures that each gene appears exactly once in the offspring, maintaining the permutation property required for valid query execution plans.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and random number generator state
- : First parent chromosome (array of Gene values representing a query execution plan)
- : Second parent chromosome (array of Gene values representing a query execution plan)
- : Output array where the resulting child chromosome will be stored
- : Length of the chromosomes (number of relations/genes in the query)
- : Auxiliary array of City structures used to track which genes have been used during crossover

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_randint](../g/geqo_randint.md) (random integer generation within specified range)
  - [Gene](../G/Gene.md) (typedef for int, represents a relation in the query)
  - [City](../C/City.md) (struct with used field to track gene utilization)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO function in src/backend/optimizer/geqo/geqo_main.c:210)

## Notes and Other Information
- This function is only compiled when the PX macro is defined during compilation
- Part of PostgreSQL's genetic algorithm-based query optimizer, used for complex queries involving many relations
- The algorithm is based on the Genitor system developed by Darrell L. Whitley at Colorado State University
- Maintains the permutation property essential for valid join orderings in query optimization
- Uses 1-based indexing for the city_table array but 0-based indexing for gene arrays

## Simplified Source

```c
void
px(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene,
   City *city_table)
{
    int num_positions;
    int i, pos, tour2_index, offspring_index;

    // Initialize city tracking table
    for (i = 1; i <= num_gene; i++)
        city_table[i].used = 0;

    // Choose random number of positions to inherit from tour1
    num_positions = geqo_randint(root, 2 * num_gene / 3, num_gene / 3);

    // Randomly select positions from tour1 to inherit
    for (i = 0; i < num_positions; i++)
    {
        pos = geqo_randint(root, num_gene - 1, 0);
        offspring[pos] = tour1[pos];
        city_table[(int) tour1[pos]].used = 1;
    }

    // Fill remaining positions with cities from tour2
    tour2_index = 0;
    offspring_index = 0;

    while (offspring_index < num_gene)
    {
        // If this position isn't filled yet
        if (!city_table[(int) tour1[offspring_index]].used)
        {
            // If current city from tour2 hasn't been used
            if (!city_table[(int) tour2[tour2_index]].used)
            {
                offspring[offspring_index] = tour2[tour2_index];
                tour2_index++;
                offspring_index++;
            }
            else
            {
                // Skip this city from tour2, it's already used
                tour2_index++;
            }
        }
        else
        {
            // This position is already filled, move to next
            offspring_index++;
        }
    }
}
```