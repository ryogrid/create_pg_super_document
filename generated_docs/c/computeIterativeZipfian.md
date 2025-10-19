# computeIterativeZipfian

## Location
[src/bin/pgbench/pgbench.c:1201-1230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1201-L1230)

## Overview
Implements a Zipfian random number generator using the rejection method for generating non-uniform random variates with parameter s > 1.0.

## Definition
```c
static int64 computeIterativeZipfian(pg_prng_state *state, int64 n, double s)
```

## Detailed Description
This function generates random integers following a Zipfian distribution using the rejection method as described in "Non-Uniform Random Variate Generation" by Luc Devroye (p. 550-551, Springer 1986). The algorithm works by repeatedly generating candidate values using inverse transform sampling until one passes the acceptance condition. The method is effective for s > 1.0 but may perform poorly when s is very close to 1.0. The function ensures that generated values are within the range [1, n].

## Parameters / Member Variables
- `state`: Pointer to the pseudo-random number generator state for generating uniform random values
- `n`: Upper bound for the generated random values; must be > 1 for meaningful distribution
- `s`: Shape parameter of the Zipfian distribution; must be > 1.0 for this implementation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_double](../p/pg_prng_double.md)
  - [pg_prng_state](../p/pg_prng_state.md) (type)
  - pow (math function)
  - floor (math function)
- Called from (representative examples):
  - [getZipfianRand](../g/getZipfianRand.md)

## Notes and Other Information
- Uses rejection sampling method which may require multiple iterations
- Performance degrades when s approaches 1.0
- Returns 1 immediately if n <= 1 (degenerate case)
- Based on established mathematical literature for non-uniform random variate generation
- Part of pgbench utility for PostgreSQL performance testing with realistic data distributions
- Located in src/bin/pgbench/pgbench.c:1201-1230

## Simplified Source

```c
static int64 computeIterativeZipfian(pg_prng_state *state, int64 n, double s) {
    // Zipfian distribution using Devroye's rejection method (s > 1.0)

    double b = pow(2.0, s - 1.0);  // Normalization constant

    if (n <= 1) return 1;  // Handle degenerate case

    while (true) {
        // Generate two uniform random variables
        double u = pg_prng_double(state);
        double v = pg_prng_double(state);

        // Candidate value using inverse transform
        double x = floor(pow(u, -1.0 / (s - 1.0)));

        // Acceptance test based on Devroye's algorithm
        double t = pow(1.0 + 1.0 / x, s - 1.0);
        if (v * x * (t - 1.0) / (b - 1.0) <= t / b && x <= n) {
            return (int64) x;  // Accept candidate
        }
        // Otherwise reject and try again
    }
}
```

**Key Points:**
- Implements Devroye's rejection method for Zipfian distribution
- Works for shape parameter s > 1.0 (performance degrades near s=1.0)
- Uses rejection sampling: generates candidates until one is accepted
- Returns values in range [1, n] following Zipfian probability