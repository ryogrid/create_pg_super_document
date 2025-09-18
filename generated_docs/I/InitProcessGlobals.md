# InitProcessGlobals

## Location
src/backend/postmaster/postmaster.c: 2034 - 2075

## Overview
Initializes process-level global variables including timestamps and random number generator seeds for secure random number generation.

## Definition
```c
void InitProcessGlobals(void)
```

## Detailed Description
InitProcessGlobals sets up fundamental process-level global state that must be initialized early in both the postmaster and every backend process. The function performs two primary tasks:

1. **Timestamp Initialization**: Sets MyStartTimestamp to the current timestamp and converts it to a time_t value stored in MyStartTime. These globals are used throughout the system to track when the process started.

2. **Random Seed Initialization**: Establishes secure random number generation by:
   - Attempting to use high-quality random bits via pg_prng_strong_seed() for the global PRNG state
   - If high-quality randomness is unavailable, falling back to a seed derived from the process ID and timestamp
   - The fallback algorithm shifts and combines timestamp bits with the PID to maximize entropy
   - Additionally seeds the deprecated random(3) function for extension compatibility

The random seed initialization ensures that each process has a different, unpredictable seed, which is crucial for security-sensitive operations and avoiding predictable behavior across processes.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp
  - timestamptz_to_time_t
  - pg_prng_strong_seed
  - pg_prng_seed
  - pg_prng_uint32
  - srandom (Unix only)
- Called from (representative examples):
  - PostmasterMain
  - InitPostmasterChild
  - InitStandaloneProcess

## Notes and Other Information
- Must be called early in both postmaster and backend processes
- The fallback seed algorithm shifts timestamp bits to maximize entropy within time windows
- Platform-specific: srandom() seeding only occurs on non-Windows systems
- Critical for security: ensures unpredictable random number generation across processes
- The random(3) seeding is maintained for extension compatibility despite being deprecated in core PostgreSQL
- The seed combines PID, shifted timestamp, and high bits to avoid predictable patterns