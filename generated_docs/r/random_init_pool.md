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
  - Chromosome (struct type for individual solutions)
  - Pool (struct type for genetic algorithm pool)
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