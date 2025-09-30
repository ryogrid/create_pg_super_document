# ox2

## Location
[src/backend/optimizer/geqo/geqo_ox2.c:49-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_ox2.c#L49-L113)

## Overview
The ox2 function implements position crossover (OX2) operation for the Genetic Query Optimizer (GEQO) in PostgreSQL, combining genetic material from two parent tours to generate an offspring tour.

## Definition

```c
void
ox2(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene, City * city_table)
```
## Detailed Description
The ox2 function performs a position crossover operation, which is one of the genetic recombination techniques used in GEQO. This crossover method creates an offspring by inheriting cities from selected positions of tour1, while filling the remaining positions with cities from tour2 in their original order (skipping cities already selected from tour1). The function randomly selects a subset of positions (between num_gene/3 and 2*num_gene/3) from tour1 to inherit, then consolidates these selections and fills remaining positions from tour2.

The algorithm works in several phases:
1. Initialize the city table to track used cities and selection list
2. Randomly determine the number of positions to inherit from tour1
3. Randomly select positions from tour1 and mark corresponding cities as used
4. Consolidate the selected cities to adjacent positions in the selection list
5. Generate the offspring by placing selected cities or inheriting from tour2

## Parameters / Member Variables
- : PlannerInfo pointer containing query planning context and random number generation state
- : First parent tour (gene sequence) contributing selected positions to offspring
- : Second parent tour (gene sequence) providing remaining cities in order
- : Output buffer to store the resulting gene sequence after crossover
- : Number of genes (cities/relations) in each tour
- : Working array of City structures used to track city usage and selection

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_randint](../g/geqo_randint.md) (for random number generation)
  - [Gene](../G/Gene.md) (typedef for int representing a city/relation)
  - [City](../C/City.md) (struct with used and select_list fields)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main genetic algorithm function in geqo_main.c:216)

## Notes and Other Information
- This is one of several crossover operators available in PostgreSQL's GEQO implementation
- The function assumes tours are permutations of integers from 1 to num_gene
- Uses the city_table as a working space to avoid conflicts and track city usage
- The random selection ensures genetic diversity while maintaining valid tour permutations
- Part of PostgreSQL's genetic algorithm approach to solving complex join ordering problems when many relations are involved in a query

## Simplified Source

```c
void
ox2(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene, City *city_table)
{
    int k, j, count, pos, select, num_positions;

    // Initialize city tracking table
    for (k = 1; k <= num_gene; k++)
    {
        city_table[k].used = 0;
        city_table[k - 1].select_list = -1;
    }

    // Randomly select how many positions to inherit from tour1
    num_positions = geqo_randint(root, 2 * num_gene / 3, num_gene / 3);

    // Randomly select positions from tour1 and mark cities as used
    for (k = 0; k < num_positions; k++)
    {
        pos = geqo_randint(root, num_gene - 1, 0);
        city_table[pos].select_list = (int) tour1[pos];
        city_table[(int) tour1[pos]].used = 1;
    }

    // Consolidate selected cities to adjacent positions
    count = 0;
    k = 0;
    while (count < num_positions)
    {
        if (city_table[k].select_list == -1)
        {
            // Find next non-empty position
            j = k + 1;
            while ((city_table[j].select_list == -1) && (j < num_gene))
                j++;

            // Move selected city to current position
            city_table[k].select_list = city_table[j].select_list;
            city_table[j].select_list = -1;
            count++;
        }
        else
            count++;
        k++;
    }

    // Generate offspring: use selected cities or inherit from tour2
    select = 0;
    for (k = 0; k < num_gene; k++)
    {
        if (city_table[(int) tour2[k]].used)
        {
            // This city was selected from tour1
            offspring[k] = (Gene) city_table[select].select_list;
            select++;
        }
        else
        {
            // Inherit from tour2
            offspring[k] = tour2[k];
        }
    }
}
```