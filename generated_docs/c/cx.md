# cx

## Location
[src/backend/optimizer/geqo/geqo_cx.c:50-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_cx.c#L50-L125)

## Overview
The  function implements cycle crossover, a genetic algorithm operator used in the GEQO (GEnetic Query Optimizer) to create offspring by combining genetic material from two parent tours.

## Definition

```c
int
cx(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring,
   int num_gene, City * city_table)
```
## Detailed Description
The  function performs cycle crossover, a specialized genetic algorithm crossover operation designed for permutation problems like the traveling salesman problem (TSP). This crossover method preserves the relative order and positions of genes from both parents while creating a valid offspring tour.

The algorithm works in three main steps:
1. **STEP 1**: Creates cycles by following the mapping between tour1 and tour2, starting from a randomly chosen position
2. **STEP 2**: Fills remaining positions with genes from tour2 if the cycle didn't create a complete tour
3. **STEP 3**: Counts differences between the first parent (tour1) and the resulting offspring for quality assessment

The cycle crossover ensures that each city appears exactly once in the offspring, maintaining the validity of the tour representation used in query optimization.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner context and random number generation state
- `*tour1`: First parent tour represented as an array of Gene values
- `*tour2`: Second parent tour represented as an array of Gene values
- `*offspring`: Output array where the resulting child tour will be stored
- `num_gene`: Number of genes (cities) in the tours
- `*city_table`: Auxiliary data structure to track city usage and positions in both parent tours
## Dependencies
- Functions called/Symbols referenced:
  -  (data type for representing genetic material)
  -  (data structure for city information)
  -  (random number generation function)
- Called from (representative examples):
  -  (main GEQO algorithm in src/backend/optimizer/geqo/geqo_main.c:201)

## Notes and Other Information
- The function returns the number of differences between tour1 and the offspring, which can be used as a quality metric
- The city_table is used to track which cities have been used and store position mappings between the two parent tours
- If the initial cycle doesn't produce a complete tour, the algorithm falls back to copying remaining cities from tour2
- This crossover operator is specifically designed for permutation-based genetic algorithms and maintains the constraint that each city appears exactly once
- The random starting position ensures diversity in the crossover operation across different calls

## Simplified Source

```c
int
cx(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring,
   int num_gene, City *city_table)
{
    int i, start_pos, curr_pos;
    int count = 0;
    int num_diffs = 0;

    // Initialize city table with position mappings
    for (i = 1; i <= num_gene; i++)
    {
        city_table[i].used = 0;
        city_table[tour2[i - 1]].tour2_position = i - 1;
        city_table[tour1[i - 1]].tour1_position = i - 1;
    }

    // Choose random starting position and begin cycle
    start_pos = geqo_randint(root, num_gene - 1, 0);
    offspring[start_pos] = tour1[start_pos];
    curr_pos = start_pos;
    city_table[(int) tour1[start_pos]].used = 1;
    count++;

    // STEP 1: Follow cycle until we return to start
    while (tour2[curr_pos] != tour1[start_pos])
    {
        city_table[(int) tour2[curr_pos]].used = 1;
        curr_pos = city_table[(int) tour2[curr_pos]].tour1_position;
        offspring[curr_pos] = tour1[curr_pos];
        count++;
    }

    // STEP 2: Fill remaining positions from tour2
    if (count < num_gene)
    {
        for (i = 1; i <= num_gene; i++)
        {
            if (!city_table[i].used)
            {
                offspring[city_table[i].tour2_position] =
                    tour2[city_table[i].tour2_position];
                count++;
            }
        }
    }

    // STEP 3: Count differences for quality metric
    if (count < num_gene)
    {
        for (i = 0; i < num_gene; i++)
            if (tour1[i] != offspring[i])
                num_diffs++;
    }

    return num_diffs;
}
```