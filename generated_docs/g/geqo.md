# geqo

## Location
src/backend/optimizer/geqo/geqo_main.c: 72 - 319

## Overview
The main entry point for PostgreSQL's Genetic Query Optimizer (GEQO) that solves the query optimization problem using genetic algorithms, treating it as a constrained Traveling Salesman Problem (TSP).

## Definition
RelOptInfo *geqo(PlannerInfo *root, int number_of_rels, List *initial_rels)

## Detailed Description
The  function implements PostgreSQL's genetic algorithm-based query optimizer for complex join problems involving many tables. When the number of relations in a query exceeds the , PostgreSQL switches from exhaustive dynamic programming to this heuristic genetic algorithm approach.

The function operates by:
1. Setting up genetic algorithm parameters (pool size and generations)
2. Creating an initial population of random chromosomes representing different join orders
3. Running iterative optimization through selection, crossover, and mutation
4. Evaluating fitness of each chromosome based on query cost
5. Returning the best query plan found after all generations

The algorithm supports multiple crossover operators (ERX, PMX, CX, PX, OX1, OX2) compiled conditionally, with Edge Recombination (ERX) being the default. Each chromosome represents a permutation of relations, and the genetic operators work to find better join orderings that minimize query execution cost.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and configuration
- : Number of relations (tables) involved in the join optimization
- : List of RelOptInfo structures representing the base relations to be joined

## Dependencies
- Functions called/Symbols referenced:
  - gimme_pool_size: Determines genetic algorithm pool size
  - gimme_number_generations: Determines number of generations to run
  - geqo_set_seed: Initializes random number generator
  - alloc_pool: Allocates memory for chromosome population
  - random_init_pool: Creates initial random population
  - sort_pool: Sorts population by fitness
  - geqo_selection: Selects parent chromosomes for breeding
  - geqo_eval: Evaluates fitness of chromosomes
  - gimme_tree: Converts best chromosome to actual query plan
  - Various crossover operators (pmx, cx, px, ox1, ox2, gimme_edge_table, gimme_tour)
  - Memory management functions (alloc_chromo, free_chromo, etc.)
- Called from (representative examples):
  - make_rel_from_joinlist: Main query planner when relation count exceeds threshold

## Notes and Other Information
- Activated when  is exceeded (default 12 relations)
- Uses different crossover methods based on compile-time flags
- Includes extensive debugging support with GEQO_DEBUG flag
- Memory management is crucial due to multiple data structures (pools, chromosomes, edge/city tables)
- The algorithm balances exploration vs exploitation through configurable selection bias
- Genetic parameters can be tuned via GUC variables (geqo_pool_size, geqo_generations, geqo_selection_bias)
- Returns NULL on failure, which triggers an ERROR in the caller