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
- `*fp`: File pointer where the output will be written
- `*pool`: Pointer to the Pool structure containing the population of chromosomes
- `start`: Starting index of chromosomes to print (inclusive)
- `stop`: Ending index of chromosomes to print (exclusive)
## Dependencies
- Functions called/Symbols referenced:
  - [Pool](../P/Pool.md) (structure type)
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

## Simplified Source

```c
void
print_pool(FILE *fp, Pool *pool, int start, int stop)
{
    int i, j;

    // Validate and correct input bounds
    if (start < 0)
        start = 0;
    if (stop > pool->size)
        stop = pool->size;

    if (start + stop > pool->size)
    {
        start = 0;
        stop = pool->size;
    }

    // Print each chromosome in the specified range
    for (i = start; i < stop; i++)
    {
        fprintf(fp, "%d)\t", i);

        // Print gene sequence
        for (j = 0; j < pool->string_length; j++)
            fprintf(fp, "%d ", pool->data[i].string[j]);

        // Print fitness value
        fprintf(fp, "%g\n", pool->data[i].worth);
    }

    fflush(fp);
}
```