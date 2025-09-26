# getExponentialRand

## Location
[src/bin/pgbench/pgbench.c:1113-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1113-L1136)

## Overview
Generates random integers following an exponential distribution within a specified inclusive range, using a configurable parameter to control the distribution curve.

## Definition

```c
static int64
getExponentialRand(pg_prng_state *state, int64 min, int64 max,
				   double parameter)
```
## Detailed Description
The  function implements an exponential probability distribution for generating random integers within the range [min, max]. The exponential distribution is characterized by a parameter that controls the rate of decay - the probability density for the cut-off value at max is exp(-parameter). This type of distribution is commonly used in performance testing to simulate real-world scenarios where certain values are more likely than others.

The implementation uses the inverse transform sampling method: it generates a uniform random value, applies the inverse of the exponential cumulative distribution function, and scales the result to fit within the specified integer range. The function includes assertions to validate that the parameter is positive and that mathematical operations are well-defined.

The exponential distribution is particularly useful in pgbench for modeling scenarios like request inter-arrival times, processing delays, or other phenomena that follow exponential patterns in real database workloads.

## Parameters / Member Variables
- : Pointer to the PRNG state structure providing the source of randomness
- : Lower bound of the output range (inclusive)  
- : Upper bound of the output range (inclusive)
- : Exponential distribution parameter (must be > 0.0) controlling the decay rate

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](../p/pg_prng_state.md) (PostgreSQL PRNG state type)
  - [pg_prng_double](../p/pg_prng_double.md) (PostgreSQL PRNG double-precision generator)
  - exp (standard C math library exponential function)
  - log (standard C math library natural logarithm function)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - [evalStandardFunc](../e/evalStandardFunc.md) (at src/bin/pgbench/pgbench.c:2724)

## Notes and Other Information
- Function is declared static, limiting its scope to the pgbench.c file
- The parameter must be positive (> 0.0) and is validated with an assertion
- Uses inverse transform sampling to convert uniform random values to exponential distribution
- The cut-off value represents exp(-parameter), which is the probability density at the maximum value
- Includes mathematical assertions to prevent division by zero and ensure valid logarithm operations
- Returns int64 values scaled to fit within the inclusive range [min, max]
- Part of pgbench's advanced random number generation capabilities for realistic workload simulation
- The uniform random value is adjusted to (0, 1] range to avoid log(0) which would be undefined
- Mathematical implementation ensures the exponential distribution properties are preserved when mapping to the integer range