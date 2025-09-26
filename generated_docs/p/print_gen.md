# print_gen

## Location
[src/backend/optimizer/geqo/geqo_misc.c:91-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_misc.c#L91-L111)

## Overview
The print_gen function outputs a summary of generation statistics for a genetic algorithm population, including best, worst, mean, and average fitness values.

## Definition

```c
void
print_gen(FILE *fp, Pool *pool, int generation)
```
## Detailed Description
This function provides a concise statistical summary of a GEQO population at a specific generation. It prints the generation number along with four key fitness metrics: the best chromosome (highest fitness), worst chromosome (lowest fitness), median chromosome (middle position), and the calculated average fitness across all chromosomes. The function assumes the pool is sorted by fitness with the best chromosome at index 0. It handles edge cases for small pool sizes and uses the second-to-last element as the worst since the last position is typically reserved as a buffer.

## Parameters / Member Variables
- : File pointer where the output will be written
- : Pointer to the Pool structure containing the sorted population of chromosomes
- : The current generation number for identification in the output

## Dependencies
- Functions called/Symbols referenced:
  - [Pool](../P/Pool.md) (structure type)
  - [avg_pool](../a/avg_pool.md) (calculates average fitness of the pool)
  - fprintf (for formatted output)
  - fflush (to ensure output is written)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO function)
  - GEQO_MISC_H (header declaration)

## Notes and Other Information
- This is a public function (non-static) available for use by other GEQO modules
- Assumes the pool is sorted in descending order of fitness (best to worst)
- Uses index 0 for best chromosome (highest fitness)
- Uses size-2 for worst chromosome to avoid the buffer position at the end
- Uses size/2 for median chromosome (middle position)
- Output format: 'generation | Best: value Worst: value Mean: value Avg: value'
- Automatically flushes output to ensure immediate visibility
- Mean refers to the median value (middle chromosome), while Avg is the true arithmetic average