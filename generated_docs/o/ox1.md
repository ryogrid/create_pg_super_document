# ox1

## Location
[src/backend/optimizer/geqo/geqo_ox1.c:49-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_ox1.c#L49-L96)

## Overview
The ox1 function implements the Order Crossover 1 (OX1) genetic algorithm crossover operator used in PostgreSQL's GEQO (Genetic Query Optimizer) for generating offspring from two parent gene sequences.

## Definition

```c
void
ox1(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene,
	City * city_table)
```
## Detailed Description
The ox1 function performs position-based crossover between two parent tours (gene sequences) to produce an offspring tour. This is a standard genetic algorithm crossover technique specifically designed for permutation problems like the Traveling Salesman Problem (TSP), which GEQO uses as an analogy for query optimization.

The algorithm works by:
1. Selecting a random contiguous segment from the first parent tour
2. Copying this segment to the same positions in the offspring
3. Filling the remaining positions with elements from the second parent tour in the order they appear, skipping elements already copied from the first parent
4. Using a circular approach to maintain the permutation property

The crossover preserves the relative order of elements from the second parent while incorporating a contiguous segment from the first parent, helping maintain genetic diversity while preserving beneficial gene sequences.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and random number generation context
- : First parent gene sequence (array of Gene elements)
- : Second parent gene sequence (array of Gene elements) 
- : Output array where the resulting offspring gene sequence is stored
- : Number of genes in the sequences (length of the arrays)
- : Array of City structures used to track which genes have been used during crossover

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_randint](../g/geqo_randint.md) (for generating random segment boundaries)
  - [Gene](../G/Gene.md) (typedef for gene representation)
  - [City](../C/City.md) (structure with 'used' field for tracking gene usage)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO algorithm function in geqo_main.c:213)

## Notes and Other Information
- This is one of several crossover operators available in PostgreSQL's GEQO implementation
- The function uses modular arithmetic to handle circular array indexing
- The city_table array must be properly sized (at least num_gene + 1 elements) and is used as temporary storage
- The crossover operator is designed to preserve the permutation property essential for TSP-like problems
- Part of the genetic algorithm suite used for optimizing complex join orders in PostgreSQL query planning

## Simplified Source

```c
void
ox1(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene,
    City *city_table)
{
    int left, right, k, p, temp;

    // Clear the city table tracking array
    for (k = 1; k <= num_gene; k++)
        city_table[k].used = 0;

    // Select random segment boundaries from first parent
    left = geqo_randint(root, num_gene - 1, 0);
    right = geqo_randint(root, num_gene - 1, 0);

    // Ensure left <= right
    if (left > right)
    {
        temp = left;
        left = right;
        right = temp;
    }

    // Copy segment from tour1 to offspring
    for (k = left; k <= right; k++)
    {
        offspring[k] = tour1[k];
        city_table[(int) tour1[k]].used = 1;
    }

    // Fill remaining positions with elements from tour2
    k = (right + 1) % num_gene;  // Start position in offspring
    p = k;                       // Start position in tour2

    while (k != left)
    {
        // If this gene from tour2 hasn't been used yet
        if (!city_table[(int) tour2[p]].used)
        {
            offspring[k] = tour2[p];
            k = (k + 1) % num_gene;
            city_table[(int) tour2[p]].used = 1;
        }
        p = (p + 1) % num_gene;
    }
}
```