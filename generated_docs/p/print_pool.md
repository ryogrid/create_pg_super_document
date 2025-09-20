# print_pool

## Location
[src/backend/optimizer/geqo/geqo_misc.c:57-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_misc.c#L57-L90)

## Overview
The print_pool function outputs formatted information about chromosomes in a genetic algorithm population pool to a specified file stream for debugging purposes.

## Definition

```c
void
print_pool(FILE *fp, Pool *pool, int start, int stop)
```
## Detailed Description
This function prints detailed information about chromosomes within a specified range of a GEQO population pool. For each chromosome in the range, it outputs the chromosome index, the complete gene sequence (string representation), and the fitness value (worth). The function includes bounds checking to ensure valid start and stop indices, automatically correcting invalid ranges. Output is formatted with each chromosome on a separate line, showing its index, gene sequence separated by spaces, and fitness value.

## Parameters / Member Variables
- : File pointer where the output will be written
- : Pointer to the Pool structure containing the population of chromosomes
- : Starting index of chromosomes to print (inclusive)
- : Ending index of chromosomes to print (exclusive)

## Dependencies
- Functions called/Symbols referenced:
  - Pool (structure type)
  - fprintf (for formatted output)
  - fflush (to ensure output is written)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main GEQO function)
  - GEQO_MISC_H (header declaration)

## Notes and Other Information
- This is a public function (non-static) available for use by other GEQO modules
- Includes robust bounds checking and automatic correction of invalid ranges
- If start < 0, it's automatically set to 0
- If stop > pool->size, it's automatically set to pool->size
- If start + stop > pool->size, both are reset to print the entire pool
- Output format: index followed by tab, gene sequence with spaces, then fitness value
- Automatically flushes the output stream to ensure immediate visibility