# geqo_selection

## Location
src/backend/optimizer/geqo/geqo_selection.c: 54 - 87

## Overview
Selects two parent chromosomes from a genetic algorithm pool for breeding purposes, using a linear bias distribution to favor higher-quality individuals.

## Definition
```c
void geqo_selection(PlannerInfo *root, Chromosome *momma, Chromosome *daddy,
                   Pool *pool, double bias)
```

## Detailed Description
This function implements parent selection for the Genetic Query Optimizer (GEQO) genetic algorithm. It selects two different chromosomes from the population pool to serve as parents for crossover operations. The selection uses a linear bias probability distribution that favors chromosomes with better fitness values (lower positions in the pool array). The function ensures that two different parents are selected unless the pool contains only one chromosome.

The selection process uses the `linear_rand()` function which implements a biased random selection where chromosomes earlier in the pool (with better fitness) have higher probability of being selected. The bias parameter controls the strength of this selection pressure.

## Parameters / Member Variables
- `root`: PlannerInfo context containing query planning information
- `momma`: Output parameter - pointer to the first selected parent chromosome
- `daddy`: Output parameter - pointer to the second selected parent chromosome  
- `pool`: Pool structure containing the population of chromosomes to select from
- `bias`: Linear bias factor controlling selection pressure (higher values favor better chromosomes)

## Dependencies
- Functions called/Symbols referenced:
  - linear_rand (generates biased random selection index)
  - geqo_copy (copies chromosome data from pool to output parameters)
- Called from (representative examples):
  - geqo (main GEQO algorithm function)

## Notes and Other Information
- The function guarantees that two different chromosomes are selected when pool size > 1
- Uses linear bias probability distribution: f(x) = bias - 2(bias - 1)x
- Part of PostgreSQL's Genetic Query Optimizer for handling large join problems
- The selected chromosomes are copied to the output parameters, not referenced directly
- Selection is biased toward chromosomes with better fitness (earlier positions in sorted pool)