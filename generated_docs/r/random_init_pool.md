# random_init_pool

## Location
[src/backend/optimizer/geqo/geqo_pool.c:91-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pool.c#L91-L134)

## Overview
Initializes a genetic algorithm pool with randomly generated valid chromosomes, each representing a potential query execution plan with an evaluated fitness score.

## Definition
```c
void random_init_pool(PlannerInfo *root, Pool *pool)
```

## Detailed Description
The `random_init_pool` function populates an allocated genetic algorithm pool with initial random chromosomes. For each chromosome in the pool, it:

1. Generates a random tour (query execution plan) using `init_tour`
2. Evaluates the fitness of the generated plan using `geqo_eval`
3. Only accepts chromosomes with valid fitness scores (< DBL_MAX)
4. Discards invalid chromosomes and generates new ones

The function includes robust error handling with a safety mechanism that prevents infinite loops by limiting failed attempts to 10,000. This ensures the genetic algorithm starts with a diverse population of valid query execution plans rather than wasting resources on invalid solutions.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing the query planning context and relation information needed for tour generation and evaluation
- `pool`: Pointer to the Pool structure to be initialized with random chromosomes

## Dependencies
- Functions called/Symbols referenced:
  - [init_tour](../i/init_tour.md) (generates random valid tours/execution plans)
  - [geqo_eval](../g/geqo_eval.md) (evaluates fitness of chromosomes)
  - elog (PostgreSQL logging function)
  - [Chromosome](../C/Chromosome.md) (struct type for individual solutions)
  - [Pool](../P/Pool.md) (struct type for genetic algorithm pool)
  - DBL_MAX (maximum double value constant)
  - DEBUG1 (debug logging level)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO optimization function during initialization)

## Notes and Other Information
- Ensures all initial chromosomes represent valid query execution plans before proceeding with genetic operations
- Uses a fail-safe mechanism to avoid infinite loops when no valid plans can be generated
- Includes debug logging to track the number of invalid tours encountered during initialization
- Critical for providing a good starting population for the genetic algorithm to evolve from
- The quality of initial random population can significantly impact the final optimization results

## Simplified Source

```c
void
random_init_pool(PlannerInfo *root, Pool *pool)
{
    Chromosome *chromo = (Chromosome *) pool->data;
    int i;
    int bad = 0;

    // Generate valid chromosomes for the pool
    i = 0;
    while (i < pool->size)
    {
        // Create random tour and evaluate its fitness
        init_tour(root, chromo[i].string, pool->string_length);
        pool->data[i].worth = geqo_eval(root, chromo[i].string, pool->string_length);

        if (pool->data[i].worth < DBL_MAX)
        {
            // Valid chromosome - keep it
            i++;
        }
        else
        {
            // Invalid chromosome - discard and try again
            bad++;
            if (i == 0 && bad >= 10000)
                elog(ERROR, "geqo failed to make a valid plan");
        }
    }

#ifdef GEQO_DEBUG
    if (bad > 0)
        elog(DEBUG1, "%d invalid tours found while selecting %d pool entries",
             bad, pool->size);
#endif
}
```