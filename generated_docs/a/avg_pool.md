# avg_pool

## Location
[src/backend/optimizer/geqo/geqo_misc.c:34-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_misc.c#L34-L56)

## Overview
The avg_pool function calculates the average fitness value (worth) of all chromosomes in a genetic algorithm population pool.

## Definition

```c
static double
avg_pool(Pool *pool)
```
## Detailed Description
This function computes the arithmetic mean of the worth values for all chromosomes in the given pool. It's specifically designed for use in debug printouts within PostgreSQL's GEQO (Genetic Query Optimizer) system. The function includes special handling to prevent overflow when the pool contains multiple occurrences of DBL_MAX values by dividing each worth value by the pool size before accumulation rather than dividing the sum at the end.

## Parameters / Member Variables
- `*pool`: A pointer to the Pool structure containing the population of chromosomes whose average fitness is to be calculated
## Dependencies
- Functions called/Symbols referenced:
  - [Pool](../P/Pool.md) (structure type)
  - elog (for error reporting)
- Called from (representative examples):
  - [print_gen](../p/print_gen.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geqo_misc.c file
- The function is primarily used for debugging purposes in GEQO
- Special overflow prevention is implemented for cases where pool contains DBL_MAX values
- The function will throw an ERROR if the pool size is zero or negative
- Performance and precision are deliberately traded off since this is only used for debug output

## Simplified Source

```c
static double
avg_pool(Pool *pool)
{
    int i;
    double cumulative = 0.0;

    if (pool->size <= 0)
        elog(ERROR, "pool_size is zero");

    // Divide by pool size before summing to prevent overflow from DBL_MAX values
    for (i = 0; i < pool->size; i++)
        cumulative += pool->data[i].worth / pool->size;

    return cumulative;
}
```