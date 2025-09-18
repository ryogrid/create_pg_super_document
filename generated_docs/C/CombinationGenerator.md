# CombinationGenerator

## Location
[src/backend/statistics/mvdistinct.c:61-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L61-L68)

## Overview
CombinationGenerator is a data structure used in PostgreSQL's multivariate statistics system to efficiently generate and iterate through all possible k-combinations of n elements for statistical analysis.

## Definition


## Detailed Description
The CombinationGenerator struct provides a mechanism for generating all possible combinations of k elements from a set of n elements. It is used in PostgreSQL's multivariate distinct statistics calculations (mvdistinct.c) to analyze relationships between different combinations of table columns.

The generator works by pre-computing all possible combinations during initialization and storing them in a flattened array. This approach trades memory for speed, allowing fast iteration through combinations without the computational overhead of generating them on-the-fly.

The combinations are stored as consecutive sequences of k integers in the combinations array, where each sequence represents one k-combination of element indices from 0 to n-1.

## Parameters / Member Variables
- : The size of each combination (number of elements to select)
- : The total number of elements to choose from (0 to n-1)
- : Index tracking the next combination to be returned by generator_next()
- : Total number of k-combinations possible (calculated as n choose k)
- : Flattened integer array storing all pre-computed combinations as consecutive k-element sequences

## Dependencies
- Functions called/Symbols referenced:
  - No direct references from the struct itself (it's a data structure)
- Called from (representative examples):
  - [generator_init](../g/generator_init.md) (creates and initializes the generator)
  - [generator_next](../g/generator_next.md) (iterates through combinations)
  - [generator_free](../g/generator_free.md) (deallocates the generator)
  - statext_ndistinct_build (uses generator for multivariate statistics)
  - [generate_combinations](../g/generate_combinations.md) (populates the combinations array)
  - [num_combinations](../n/num_combinations.md) (calculates the total number of combinations)

## Notes and Other Information
- The generator is specifically designed for use in PostgreSQL's extended statistics system for analyzing column dependencies
- All combinations are pre-computed during initialization, making iteration very fast but requiring O(n choose k * k) memory
- The combinations array stores k-element sequences consecutively, so combination i starts at index i*k
- Used primarily in mvdistinct.c for calculating multivariate distinct value statistics
- The generator assumes 0-based indexing for elements (0 to n-1)
- Proper resource management requires calling generator_free() to avoid memory leaks