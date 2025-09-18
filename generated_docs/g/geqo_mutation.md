# geqo_mutation

## Location
[src/backend/optimizer/geqo/geqo_mutation.c:43-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_mutation.c#L43-L68)

## Overview
The geqo_mutation function implements a mutation operator for the Genetic Query Optimizer (GEQO) in PostgreSQL, performing random swaps between genes in a tour to introduce genetic diversity during the evolutionary optimization process.

## Definition


## Detailed Description
The geqo_mutation function is part of PostgreSQL's Genetic Query Optimizer (GEQO) system, which uses genetic algorithms to solve complex join ordering problems. This function implements the mutation operation, one of the fundamental genetic operators used to maintain diversity in the population and prevent premature convergence.

The mutation process works by:
1. Determining a random number of swaps to perform (up to num_gene/3)
2. For each swap, selecting two different random positions in the gene tour
3. Exchanging the genes at these positions
4. Repeating until all planned swaps are completed

This function is conditionally compiled and only available when the CX (Cycle Crossover) recombination method is selected, as indicated by the #if defined(CX) preprocessor directive. The implementation is based on algorithms from the Genitor genetic algorithm system developed at Colorado State University.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and access to the GEQO private data including the random state
- : Array of Gene elements representing the current join order solution (modified in-place)
- : Integer specifying the number of genes (relations) in the tour array

## Dependencies
- Functions called/Symbols referenced:
  - [geqo_randint](geqo_randint.md): Generates random integers within specified ranges using the GEQO random state
  - Gene: Typedef for int, representing a relation identifier in the genetic algorithm
- Called from (representative examples):
  - [geqo](geqo.md): Main GEQO algorithm in geqo_main.c:206

## Notes and Other Information
- This function is only compiled when CX (Cycle Crossover) mode is enabled through preprocessor definitions
- The mutation rate is adaptive, with the number of swaps being a random value between 0 and num_gene/3
- The function ensures that swap positions are always different by re-randomizing swap2 if it equals swap1
- The implementation is derived from the Genitor genetic algorithm system and maintains the original copyright attribution
- Gene is defined as a simple int typedef in geqo_gene.h, representing relation identifiers
- The random number generation uses PostgreSQL's internal PRNG system via geqo_randint, ensuring reproducible results when needed