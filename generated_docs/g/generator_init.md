# generator_init

## Location
[src/backend/statistics/mvdistinct.c:589-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L589-L626)

## Overview
Initializes a generator that produces combinations of K elements from the interval (0..N).

## Definition
```c
static CombinationGenerator *generator_init(int n, int k)
```

## Detailed Description
This function creates and initializes a CombinationGenerator structure that pre-computes all possible combinations of k elements from n total elements. Rather than generating combinations on-the-fly, this approach pre-builds all combinations during initialization for simpler access patterns. The function allocates memory for both the generator state and all combinations, then uses generate_combinations to populate the combination data. The generator is reset to start from the first combination after initialization.

## Parameters / Member Variables  
- `n`: The total number of elements (upper bound of interval 0..N)
- `k`: The number of elements in each combination

## Dependencies
- Functions called/Symbols referenced:
  - [CombinationGenerator](../C/CombinationGenerator.md) (structure type)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [n_choose_k](../n/n_choose_k.md) (computes binomial coefficient)
  - [generate_combinations](generate_combinations.md) (populates the combinations)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [statext_ndistinct_build](../s/statext_ndistinct_build.md)

## Notes and Other Information
- The function is declared as static, limiting scope to mvdistinct.c
- Uses PostgreSQL's palloc for memory allocation within the current memory context
- Pre-generates all combinations rather than computing them lazily for performance
- Includes assertions to validate input parameters (n >= k and k > 0) and verify correct generation
- Memory is allocated as a single chunk for the generator state, with separate allocation for combinations array
- After generation, resets the current index to 0 to begin iteration from the first combination
- Located in src/backend/statistics/mvdistinct.c, used for multivariate distinct value statistics

## Simplified Source

```c
static CombinationGenerator *
generator_init(int n, int k)
{
    CombinationGenerator *state;

    // Allocate generator state
    state = (CombinationGenerator *) palloc(sizeof(CombinationGenerator));

    // Calculate total number of combinations and allocate space
    state->ncombinations = n_choose_k(n, k);
    state->combinations = (int *) palloc(sizeof(int) * k * state->ncombinations);

    // Initialize state
    state->current = 0;
    state->k = k;
    state->n = n;

    // Generate all combinations
    generate_combinations(state);

    // Reset to start from first combination
    state->current = 0;

    return state;
}
```