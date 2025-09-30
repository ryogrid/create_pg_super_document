# gimme_pool_size

## Location
[src/backend/optimizer/geqo/geqo_main.c:320-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_main.c#L320-L351)

## Overview
Determines the genetic algorithm population size for GEQO, returning either the configured value or a calculated default based on query complexity and effort settings.

## Definition
static int gimme_pool_size(int nr_rel)

## Detailed Description
The `gimme_pool_size` function calculates the optimal population size for PostgreSQL's genetic algorithm optimizer. The population size directly affects the quality of optimization results and computational cost.

The function uses the following logic:
1. If `Geqo_pool_size` is explicitly configured (>= 2), returns that value
2. Otherwise, calculates a default using the formula: 2^(nr_rel + 1)
3. Constrains the result within bounds based on `Geqo_effort`:
   - Minimum: 10 * Geqo_effort (10 to 100 individuals)
   - Maximum: 50 * Geqo_effort (50 to 500 individuals)

This adaptive sizing ensures that complex queries with many relations get larger populations for better optimization, while simpler queries use smaller populations for efficiency. The effort parameter allows users to trade optimization quality for speed.

## Parameters / Member Variables
- `nr_rel`: Number of relations (tables) involved in the join optimization problem

## Dependencies
- Functions called/Symbols referenced:
  - pow: Mathematical power function for exponential calculation
  - ceil: Ceiling function to round up to nearest integer
  - Geqo_pool_size: Global configuration variable for explicit pool size
  - Geqo_effort: Global configuration variable controlling optimization effort (1-10)
- Called from (representative examples):
  - [geqo](geqo.md): Main GEQO function during genetic algorithm setup

## Notes and Other Information
- Minimum legal pool size is 2 (required for genetic algorithm selection)
- Default formula 2^(n+1) provides exponential growth with relation count
- Effort-based bounds prevent excessive computation time or inadequate optimization
- [Pool](../P/Pool.md) size directly impacts memory usage and algorithm convergence time
- Larger pools generally find better solutions but take longer to execute
- The function is static, only accessible within the geqo_main.c module

## Simplified Source

```c
static int gimme_pool_size(int nr_rel) {
    double size;
    int minsize, maxsize;

    // Use configured pool size if valid (>= 2)
    if (Geqo_pool_size >= 2) {
        return Geqo_pool_size;
    }

    // Calculate default size: 2^(nr_rel + 1)
    size = pow(2.0, nr_rel + 1.0);

    // Apply effort-based constraints
    maxsize = 50 * Geqo_effort;  // 50 to 500 individuals
    if (size > maxsize) {
        return maxsize;
    }

    minsize = 10 * Geqo_effort;  // 10 to 100 individuals
    if (size < minsize) {
        return minsize;
    }

    return (int) ceil(size);
}
```