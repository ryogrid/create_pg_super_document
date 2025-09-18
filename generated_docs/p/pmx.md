# pmx

## Location
src/backend/optimizer/geqo/geqo_pmx.c: 49 - 220

## Overview
The  function implements the Partially Matched Crossover (PMX) genetic algorithm operator for the GEQO (Genetic Query Optimizer) in PostgreSQL, used to create offspring solutions by combining two parent tour solutions.

## Definition


## Detailed Description
The PMX (Partially Matched Crossover) function is a sophisticated genetic crossover operator that creates a new offspring tour by combining genetic material from two parent tours while maintaining the validity of the solution. This algorithm is specifically designed for permutation-based genetic algorithms where each gene must appear exactly once in the solution.

The algorithm works in three main steps:
1. **Initial Setup**: Randomly selects crossover points and copies one parent (tour2) to the offspring
2. **Primary Crossover**: Replaces the segment between crossover points with material from the other parent (tour1)
3. **Conflict Resolution**: Handles duplicates and missing genes through a sophisticated mapping and replacement mechanism

The function ensures that the resulting offspring is a valid permutation where each gene appears exactly once, making it suitable for optimization problems like query join order optimization.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and random number generation state
- : First parent tour (gene sequence) - acts as the "MOM" in the crossover
- : Second parent tour (gene sequence) - acts as the "DAD" in the crossover  
- : Output array where the resulting offspring tour will be stored
- : Number of genes in each tour (length of the gene sequences)

## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation for internal arrays
  - : Memory deallocation for cleanup
  - : Random integer generation for crossover point selection
  - : Type definition for individual genes in the tour
  - , : Constants used to track gene origin during crossover
- Called from (representative examples):
  - : Main GEQO algorithm function in geqo_main.c:198

## Notes and Other Information
- The PMX algorithm is particularly well-suited for permutation problems where order matters and no duplicates are allowed
- Uses a three-step process with sophisticated conflict resolution to maintain solution validity
- Allocates temporary arrays (, , , ) for tracking gene mappings and conflicts
- The crossover points are selected randomly, with proper handling when left > right
- Part of PostgreSQL's genetic query optimization system, used to optimize complex multi-table joins
- Memory management is handled properly with palloc/pfree pairs for all temporary allocations
- The algorithm maintains a check_list to ensure each gene appears exactly once in the final offspring