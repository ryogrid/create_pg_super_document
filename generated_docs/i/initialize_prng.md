# initialize_prng

## Location
[src/backend/utils/adt/pseudorandomfuncs.c:34-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudorandomfuncs.c#L34-L61)

## Overview
Initializes (seeds) the pseudo-random number generator (PRNG) if it has not been seeded yet in the current process.

## Definition


## Detailed Description
This internal function ensures the global PRNG state is properly initialized before any random number generation occurs. It uses a two-tier seeding approach:

1. **Primary approach**: Attempts to seed using high-quality random bits via 
2. **Fallback approach**: If high-quality seeding fails, creates a seed by combining the current timestamp with the process ID (PID)

The function uses the  global flag to ensure initialization occurs only once per process. The fallback seed combines temporal unpredictability (timestamp) with process uniqueness (PID) by XORing the timestamp with the PID shifted left by 32 bits.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Attempts to seed with cryptographically strong random data
  -  - Gets current timestamp for fallback seeding
  -  - Seeds the PRNG with a 64-bit value
- Called from:
  -  - Random float generation
  -  - Normal distribution random generation  
  -  - 32-bit integer random generation
  -  - 64-bit integer random generation
  -  - Numeric type random generation

## Notes and Other Information
- This is a static function, internal to the pseudorandomfuncs.c module
- Uses  macro for branch prediction optimization on the rare initialization path
- The fallback seeding strategy ensures deterministic behavior across processes while maintaining reasonable unpredictability
- The function is thread-safe assuming single-threaded access to the global  and  variables