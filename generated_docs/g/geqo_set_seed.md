# geqo_set_seed

## Location
[src/backend/optimizer/geqo/geqo_random.c:20-27](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_random.c#L20-L27)

## Overview
Initializes the random number generator seed for the Genetic Query Optimizer (GEQO) to ensure reproducible random behavior during query optimization.

## Definition


## Detailed Description
The  function initializes the pseudo-random number generator state used by the GEQO algorithm. It extracts the private GEQO data from the planner's join search context and uses PostgreSQL's  function to set the random seed. This ensures that GEQO can produce reproducible results when the same seed is used, which is crucial for testing and debugging query optimization behavior.

The function operates on the GEQO private data structure stored within the planner's join search private context, maintaining proper encapsulation of the random state within the GEQO subsystem.

## Parameters / Member Variables
- : Pointer to the PlannerInfo structure containing query planning context and private data
- : Double-precision floating-point value used to initialize the random number generator

## Dependencies
- Functions called/Symbols referenced:
  - GeqoPrivateData (struct type)
  - pg_prng_fseed (PostgreSQL's pseudo-random number generator seed function)
- Called from (representative examples):
  - [geqo](geqo.md) (main GEQO entry point in geqo_main.c:106)

## Notes and Other Information
- Part of the GEQO (Genetic Query Optimizer) subsystem in PostgreSQL
- The function assumes that the GEQO private data has been properly initialized before being called
- The random seed affects the genetic algorithm's behavior, including mutation, crossover, and selection operations
- Proper seeding is essential for reproducible query optimization results in testing scenarios