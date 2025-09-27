# PostmasterIsAlive

## Location
[src/include/storage/pmsignal.h:95-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/pmsignal.h#L95-L102)

## Overview
PostmasterIsAlive is an optimized inline function that checks whether the postmaster process is still alive, using a fast-path optimization to avoid expensive system calls in the common case.

## Definition
```c
static inline bool PostmasterIsAlive(void)
```

## Detailed Description
PostmasterIsAlive provides an efficient mechanism to determine if the postmaster process is still running. The function implements a two-tier checking strategy:

1. **Fast path**: On platforms that support postmaster death signals (USE_POSTMASTER_DEATH_SIGNAL), it first checks the `postmaster_possibly_dead` flag. If this flag is false (the common case), it immediately returns true without making any system calls.

2. **Slow path**: If the flag indicates the postmaster might be dead, it calls `PostmasterIsAliveInternal()` to perform the actual liveness check using platform-specific mechanisms (pipe reading on Unix, WaitForSingleObject on Windows).

On platforms that do not support postmaster death signals, PostmasterIsAlive is simply defined as a macro that directly calls PostmasterIsAliveInternal().

The optimization relies on the `likely()` compiler hint to indicate that the postmaster being alive is the expected case, allowing the compiler to optimize the branch prediction accordingly.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value:
- **Return value**: `true` if the postmaster process is alive, `false` if it has died

## Dependencies
- Functions called/Symbols referenced:
  - `likely()` (compiler optimization hint)
  - `[PostmasterIsAliveInternal](PostmasterIsAliveInternal.md)()` (actual liveness checking logic)
  - `postmaster_possibly_dead` (volatile flag set by signal handler)

- Called from (representative examples):
  - [vacuum_delay_point](../v/vacuum_delay_point.md) (src/backend/commands/vacuum.c:2439)
  - [pgarch_ArchiverCopyLoop](../p/pgarch_ArchiverCopyLoop.md) (src/backend/postmaster/pgarch.c:410)
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md) (src/backend/postmaster/startup.c:185)
  - [WaitEventAdjustKqueue](../W/WaitEventAdjustKqueue.md) (src/backend/storage/ipc/latch.c:1354)

## Notes and Other Information
- This function is declared as `static inline` in the header file (src/include/storage/pmsignal.h:95-102) for maximum performance
- The function is conditionally compiled based on USE_POSTMASTER_DEATH_SIGNAL support
- On platforms without death signal support, this becomes a direct call to PostmasterIsAliveInternal()
- The `postmaster_possibly_dead` flag is of type `volatile sig_atomic_t` to ensure thread-safe access from signal handlers
- This is a critical function for detecting postmaster death in background processes, allowing them to terminate gracefully when the postmaster dies

## Simplified Source

```c
// Simplified version of PostmasterIsAlive
static inline bool PostmasterIsAlive(void) {
    // Fast path: Check the signal-based flag first
    // This avoids expensive system calls in the common case
    if (likely(!postmaster_possibly_dead)) {
        return true;  // Postmaster is alive (common case)
    }

    // Slow path: Actually check if postmaster is alive
    // using platform-specific mechanisms
    return PostmasterIsAliveInternal();
}
```

Key simplifications made:
- Added inline comments explaining the two-tier checking strategy
- Clarified the purpose of the fast path optimization
- Explained the likely() compiler hint for branch prediction
- Maintained the exact same logic while making the optimization strategy clear
- Preserved the performance-critical nature of this function