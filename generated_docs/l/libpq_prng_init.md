# libpq_prng_init

## Location
src/interfaces/libpq/fe-connect.c: 1093 - 1119

## Overview
Initializes the pseudo-random number generator state for a libpq connection, using high-quality random bits when available or falling back to a deterministic seed based on connection properties.

## Definition
```c
static void libpq_prng_init(PGconn *conn)
```

## Detailed Description
This function initializes the prng_state field of a PGconn structure to provide unpredictable random number generation for the connection. It first attempts to use pg_prng_strong_seed() to obtain high-quality cryptographic randomness. If that fails (typically when cryptographic random sources are unavailable), it constructs a fallback seed by combining several semi-unpredictable values: the connection pointer address, process ID, and current timestamp (both seconds and microseconds). These values are XORed together to create a reasonably unpredictable seed for the PRNG.

## Parameters / Member Variables
- `conn`: PGconn structure whose prng_state field will be initialized

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_strong_seed (attempts to seed with cryptographically strong randomness)
  - gettimeofday (gets current time for fallback seed)
  - pg_prng_seed (initializes PRNG with the computed seed)
  - getpid (gets process ID for fallback seed)
- Called from (representative examples):
  - pqConnectOptions2

## Notes and Other Information
- Prioritizes security by attempting to use strong random sources first
- Fallback seed combines multiple sources of entropy to improve unpredictability
- Uses XOR operations to mix the entropy sources in the fallback case
- The connection pointer address provides some uniqueness across different connections
- Process ID and timestamp provide temporal and process-specific variation
- Essential for security-sensitive operations that require random numbers in libpq
- Location: src/interfaces/libpq/fe-connect.c:1093-1119