# HandleCatchupInterrupt

## Location
[src/backend/storage/ipc/sinval.c:155-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinval.c#L155-L174)

## Overview
HandleCatchupInterrupt is a signal handler function that responds to PROCSIG_CATCHUP_INTERRUPT signals by setting a flag for deferred processing of shared invalidation catchup events.

## Definition
```c
void HandleCatchupInterrupt(void)
```

## Detailed Description
HandleCatchupInterrupt serves as the signal handler for PROCSIG_CATCHUP_INTERRUPT signals in PostgreSQL's shared invalidation system. When a backend process falls behind in processing shared invalidation messages, other processes can send this signal to trigger a catchup operation. However, since signal handlers must be very careful about what operations they perform, this function does not directly process the catchup events.

Instead, it follows a two-step approach: it sets the catchupInterruptPending flag to indicate that catchup processing is needed, and then calls SetLatch(MyLatch) to wake up the main process loop so it can handle the catchup processing at a safe time outside of signal context. This design pattern is commonly used in PostgreSQL to defer complex operations from signal handlers to the main execution context where it's safe to perform database operations.

The function includes a prominent comment warning that it's called by a signal handler, emphasizing the need for signal-safe operations only.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md) (wakes up the process to handle the pending interrupt)
  - catchupInterruptPending (global flag variable, set to true)
  - MyLatch (process's latch for inter-process communication)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md) (main signal dispatcher for SIGUSR1 signals)

## Notes and Other Information
- This function must be signal-safe since it runs in signal handler context
- The function replaced an older design that called ProcessCatchupEvent directly when idle
- The deferred processing approach is safer and more reliable than immediate processing in signal context
- The catchup mechanism helps ensure that no backend falls too far behind in processing shared invalidation messages
- PROCSIG_CATCHUP_INTERRUPT is part of PostgreSQL's inter-process signaling system
- The actual catchup processing is handled later by ProcessCatchupInterrupt when it's safe to do so

## Simplified Source

```c
// Simplified version of HandleCatchupInterrupt
void HandleCatchupInterrupt(void) {
    // Step 1: Mark that catchup processing is needed
    // This flag will be checked later in the main process loop
    catchupInterruptPending = true;

    // Step 2: Wake up the main process to handle the pending work
    // SetLatch ensures the process will check for pending interrupts
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Focused on the two core operations: setting flag and waking process
- Added explanatory comments for each step
- Removed signal handler safety warnings (kept in original for reference)
- Emphasized the deferred processing pattern used throughout PostgreSQL