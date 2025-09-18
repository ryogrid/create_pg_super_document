# geqo_randint

## Location
src/backend/optimizer/geqo/geqo_random.c: 36 - 45

## Overview
Generates a pseudo-random integer within a specified range for use in the Genetic Query Optimizer (GEQO) algorithm's discrete selection operations.

## Definition
```c
int geqo_randint(PlannerInfo *root, int upper, int lower)
```

## Detailed Description
The `geqo_randint` function generates a uniformly distributed random integer within the specified range [lower, upper]. It accesses the GEQO-specific random state from the planner's private data and uses PostgreSQL's `pg_prng_uint64_range` function to generate the random value, then casts it to an integer.

This function is extensively used throughout the GEQO algorithm for operations that require discrete random choices, such as selecting random positions for crossover operations, choosing genes for mutation, selecting random tours, and making other index-based decisions in genetic algorithms. The current implementation assumes that the lower bound is never negative, allowing for direct use of the unsigned range function.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing the query planning context and GEQO private data
- `upper`: Upper bound of the random range (inclusive)
- `lower`: Lower bound of the random range (inclusive, assumed to be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - GeqoPrivateData (struct type)
  - pg_prng_uint64_range (PostgreSQL's pseudo-random integer range generator)
- Called from (representative examples):
  - cx (crossover operation in geqo_cx.c:68)
  - gimme_tour, gimme_gene, edge_failure (in geqo_erx.c for edge recombination)
  - geqo_mutation (mutation operations in geqo_mutation.c:47,53,54,57)
  - ox1, ox2 (order crossover operations)
  - pmx (partially matched crossover)
  - px (position-based crossover)
  - init_tour (tour initialization in geqo_recombination.c:53)

## Notes and Other Information
- Returns an integer in the range [lower, upper] (both bounds inclusive)
- The function assumes lower >= 0 in current usage patterns
- Widely used across all GEQO genetic operators for discrete random selections
- Essential for position-based operations in genetic algorithms such as crossover point selection and gene indexing
- The random state must be properly initialized before calling this function
- Performance-critical function as it's called frequently during genetic algorithm execution