# GeqoPrivateData

## Location
[src/include/optimizer/geqo.h:79-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/geqo.h#L79-L90)

## Overview
GeqoPrivateData is a structure that encapsulates private state data for a GEQO (Genetic Algorithm Query Optimizer) optimization run, providing context and random number generation state for genetic algorithm operations during query planning.

## Definition

```c
typedef struct
{
	List	   *initial_rels;	/* the base relations we are joining */
	pg_prng_state random_state; /* PRNG state */
} GeqoPrivateData;
```
## Detailed Description
GeqoPrivateData serves as a private data container for the GEQO genetic algorithm optimizer within PostgreSQL's query planner. This structure is stored in the PlannerInfo's join_search_private field during GEQO execution and provides essential context for the genetic algorithm operations. The structure maintains two critical pieces of information: the list of base relations that need to be joined and the pseudo-random number generator state that ensures reproducible and controlled randomness throughout the genetic algorithm iterations.

The GEQO algorithm uses this private data structure to maintain state consistency across multiple genetic algorithm operations including population initialization, crossover operations, mutation, and fitness evaluation. The random_state field is particularly important as it allows for deterministic behavior when a specific seed is set, enabling reproducible query plans for testing and debugging purposes.

## Parameters / Member Variables
- : A List containing the base relations (tables) that need to be joined in the query. This provides the genetic algorithm with the fundamental building blocks for constructing join orders.
- : A pg_prng_state structure that maintains the state of the pseudo-random number generator used throughout the genetic algorithm operations, ensuring consistent and reproducible randomness.

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL's list data structure)
  - pg_prng_state (PostgreSQL's PRNG state structure)
- Called from (representative examples):
  - geqo (main GEQO entry point function)
  - geqo_set_seed (initializes the random state)
  - geqo_rand (accesses random state for generating random numbers)
  - geqo_randint (accesses random state for generating random integers)
  - gimme_tree (uses the structure for tree construction)

## Notes and Other Information
- The structure is allocated on the stack in the geqo() function and its address is stored in root->join_search_private for access by other GEQO functions
- The random_state field is initialized using geqo_set_seed() with either a user-specified seed (Geqo_seed) or a default seed
- This private data structure is only used during GEQO optimization and is not persisted beyond the query planning phase
- The structure enables thread-safe random number generation within the GEQO algorithm by maintaining separate PRNG state per optimization run
- GEQO is typically used for queries involving many tables (controlled by geqo_threshold parameter) where exhaustive join order enumeration would be too expensive