# sampler_random_fract

## Location
[src/backend/utils/misc/sampling.c:241-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/sampling.c#L241-L265)

## Overview
Generates a uniformly distributed random floating-point value in the range (0, 1), ensuring the result is never exactly 0.0.

## Definition
```c
double sampler_random_fract(pg_prng_state *randstate)
```

## Detailed Description
This function provides a random floating-point number generator specifically designed for sampling algorithms. It uses PostgreSQL's internal PRNG to generate values in the range [0.0, 1.0) and then rejects any result that equals exactly 0.0, ensuring the returned value is always in the open interval (0, 1). This is crucial for sampling algorithms that require strictly positive probabilities and avoid division by zero or logarithm of zero operations.

## Parameters / Member Variables
- `randstate`: Pointer to an initialized pg_prng_state structure containing the random number generator state

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_double](../p/pg_prng_double.md)
  - [pg_prng_state](../p/pg_prng_state.md) (type)
  - [ReservoirStateData](../R/ReservoirStateData.md) (type)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md)
  - [BlockSampler_Next](../B/BlockSampler_Next.md)
  - [reservoir_init_selection_state](../r/reservoir_init_selection_state.md)
  - [reservoir_get_next_S](../r/reservoir_get_next_S.md)
  - [anl_random_fract](../a/anl_random_fract.md)
  - [anl_init_selection_state](../a/anl_init_selection_state.md)

## Notes and Other Information
The function uses a do-while loop with the unlikely() macro to optimize for the common case where pg_prng_double() doesn't return 0.0. This approach ensures mathematical correctness for sampling algorithms while maintaining good performance. The function is widely used throughout PostgreSQL's sampling infrastructure, including table analysis, block sampling, and reservoir sampling algorithms.

## Simplified Source

```c
double
sampler_random_fract(pg_prng_state *randstate)
{
    double result;

    // Generate random value in [0.0, 1.0) and reject 0.0 to ensure (0, 1)
    do {
        result = pg_prng_double(randstate);
    } while (unlikely(result == 0.0));

    return result;
}
```