# InitProcessGlobals

## Location
[src/backend/postmaster/postmaster.c:2034-2075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2034-L2075)

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

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [timestamptz_to_time_t](../t/timestamptz_to_time_t.md)
  - pg_prng_strong_seed
  - [pg_prng_seed](../p/pg_prng_seed.md)
  - [pg_prng_uint32](../p/pg_prng_uint32.md)
  - srandom (Unix only)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [InitPostmasterChild](InitPostmasterChild.md)
  - [InitStandaloneProcess](InitStandaloneProcess.md)

## Notes and Other Information
- Must be called early in both postmaster and backend processes
- The fallback seed algorithm shifts timestamp bits to maximize entropy within time windows
- Platform-specific: srandom() seeding only occurs on non-Windows systems
- Critical for security: ensures unpredictable random number generation across processes
- The random(3) seeding is maintained for extension compatibility despite being deprecated in core PostgreSQL
- The seed combines PID, shifted timestamp, and high bits to avoid predictable patterns

## Simplified Source

```c
// Simplified version of InitProcessGlobals
void InitProcessGlobals(void) {
    // Step 1: Initialize process start time globals
    MyStartTimestamp = GetCurrentTimestamp();
    MyStartTime = timestamptz_to_time_t(MyStartTimestamp);

    // Step 2: Set up secure random number generation
    if (!pg_prng_strong_seed(&pg_global_prng_state)) {
        // Fallback: create seed from PID and timestamp
        uint64 seed = MyProcPid ^
                     (MyStartTimestamp << 12) ^
                     (MyStartTimestamp >> 20);
        pg_prng_seed(&pg_global_prng_state, seed);
    }

    // Step 3: Initialize legacy random() for extension compatibility
#ifndef WIN32
    srandom(pg_prng_uint32(&pg_global_prng_state));
#endif
}
```

Key simplifications made:
- Removed detailed comments about bit manipulation rationale
- Consolidated the fallback seed calculation into a single expression
- Simplified variable declarations and assignments
- Focused on the three main logical steps
- Maintained the essential algorithm and platform-specific behavior