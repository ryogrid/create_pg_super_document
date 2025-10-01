# geqo_rand

## Location
[src/backend/optimizer/geqo/geqo_random.c:28-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_random.c#L28-L35)

## Overview
Generates a pseudo-random double-precision floating-point number between 0.0 and 1.0 for use in the Genetic Query Optimizer (GEQO) algorithm.

## Definition
```c
double geqo_rand(PlannerInfo *root)
```

## Detailed Description
The `geqo_rand` function provides a source of randomness for the GEQO algorithm by generating uniformly distributed random double values in the range [0.0, 1.0). It accesses the GEQO-specific random state stored in the planner's private data and uses PostgreSQL's `pg_prng_double` function to generate the random number.

This function is fundamental to the genetic algorithm's operation, providing the randomness needed for various genetic operations such as selection, crossover probability determination, and mutation decisions. The use of a dedicated random state ensures that GEQO's randomness is isolated from other parts of the system and can be controlled independently.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing the query planning context and GEQO private data with the random state

## Dependencies
- Functions called/Symbols referenced:
  - [GeqoPrivateData](../G/GeqoPrivateData.md) (struct type)
  - [pg_prng_double](../p/pg_prng_double.md) (PostgreSQL's pseudo-random double generator function)
- Called from (representative examples):
  - [linear_rand](../l/linear_rand.md) (in geqo_selection.c:104 for selection operations)

## Notes and Other Information
- Returns a double-precision value in the range [0.0, 1.0)
- The random state must be properly initialized (typically via geqo_set_seed) before calling this function
- Essential for implementing probabilistic decisions in genetic algorithm operations
- Part of the GEQO subsystem's random number generation infrastructure
- The quality of randomness directly affects the effectiveness of the genetic optimization process

## Simplified Source

```c
double geqo_rand(PlannerInfo *root) {
    // Access GEQO private data containing random state
    GeqoPrivateData *private = (GeqoPrivateData *) root->join_search_private;

    // Generate random double in range [0.0, 1.0)
    return pg_prng_double(&private->random_state);
}
```