# init_tour

## Location
src/backend/optimizer/geqo/geqo_recombination.c: 34 - 68

## Overview
Randomly generates a legal "traveling salesman" tour where each point is visited only once, used in PostgreSQL's genetic query optimizer (GEQO).

## Definition


## Detailed Description
This function creates a random permutation of numbers from 1 to num_gene to represent a valid tour for the traveling salesman problem, which is the underlying optimization problem in GEQO. It implements the "inside-out" variant of the Fisher-Yates shuffle algorithm to generate the permutation efficiently in a single pass.

The algorithm works by building the permutation incrementally: for each position i, it selects a random index j from 0 to i, then places the new value (i+1) at position j while moving the previous value at j to position i. This ensures each generated permutation has equal probability.

## Parameters / Member Variables
- : PlannerInfo pointer containing query planning context and random number generation state
- : Output array of Gene values that will contain the generated tour permutation
- : Number of genes (cities) in the tour, determines the size of the permutation

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_randint](../g/geqo_randint.md) (generates random integers within specified range)
  - Gene (type representing individual elements in genetic algorithm)
  - City (related type used in GEQO context)
- Called from (representative examples):
  - [random_init_pool](../r/random_init_pool.md) (initializes population pool with random tours)

## Notes and Other Information
- Part of PostgreSQL's Genetic Query Optimizer (GEQO) subsystem
- Uses Fisher-Yates shuffle variant for optimal randomness and performance  
- The tour represents a join order in query optimization context
- Critical for generating diverse initial population in genetic algorithm
- Handles edge case of num_gene > 0 by explicitly setting first element