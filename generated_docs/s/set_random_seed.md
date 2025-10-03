# set_random_seed

## Location
[src/bin/pgbench/pgbench.c:6607-6652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L6607-L6652)

## Overview
Initializes the random number generator seed for pgbench based on the provided seed parameter and sets up the base random sequence for other random number generators.

## Definition

```c
static bool
set_random_seed(const char *seed)
```
## Detailed Description
This function sets up the random seed for pgbench's pseudo-random number generation system. It supports three types of seed inputs: time-based seeding (NULL or "time"), strong random seeding ("rand"), and explicit numeric seeding. The function parses the seed parameter, generates the appropriate seed value, logs the seed being used, stores it in the global  variable, and initializes the  using PostgreSQL's PRNG system. This base sequence is then used to initialize other random sequences throughout pgbench's execution.

## Parameters / Member Variables
- `*seed`: String parameter specifying the seed type or value. NULL or "time" uses current timestamp, "rand" uses cryptographically strong random source, numeric string uses that value as seed
## Dependencies
- Functions called/Symbols referenced:
  - [pg_time_now](../p/pg_time_now.md) (for time-based seeding)
  - [pg_strong_random](../p/pg_strong_random.md) (for strong random seeding)
  - pg_log_error_detail (for detailed error messages)
  - pg_log_info (for logging seed information)
  - [pg_prng_seed](../p/pg_prng_seed.md) (for initializing the base random sequence)
- Called from (representative examples):
  - [main](../m/main.md) (in pgbench.c at lines 6770 and 6989)

## Notes and Other Information
- Returns false on error (invalid seed format or strong random generation failure)
- Sets global variables  and initializes 
- Uses sscanf for parsing numeric seeds with garbage detection
- Logs the actual seed value used when explicitly specified
- Part of pgbench's deterministic random number generation system for reproducible benchmarks