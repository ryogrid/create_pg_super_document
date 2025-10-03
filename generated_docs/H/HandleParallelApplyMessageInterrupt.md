# HandleParallelApplyMessageInterrupt

## Location
[src/backend/replication/logical/applyparallelworker.c:989-1000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L989-L1000)

## Overview
HandleParallelApplyMessageInterrupt is a signal-safe interrupt handler that sets flags to indicate a parallel apply worker message is pending processing.

## Definition
```c
void HandleParallelApplyMessageInterrupt(void)
```

## Detailed Description
This function serves as a signal handler for the PROCSIG_PARALLEL_APPLY_MESSAGE signal in PostgreSQL's logical replication system. It is designed to be called within a signal handler context, which severely restricts the operations it can safely perform.

The function sets two critical flags: InterruptPending (which is part of PostgreSQL's general interrupt handling mechanism) and ParallelApplyMessagePending (which is specific to parallel apply worker message handling). These flags will be checked during the next CHECK_FOR_INTERRUPTS() call, triggering the actual message processing via HandleParallelApplyMessages().

Finally, it calls SetLatch() to wake up any process that might be waiting on the latch, ensuring timely processing of the pending parallel apply messages.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - InterruptPending (global variable)
  - ParallelApplyMessagePending (global variable)
  - [SetLatch](../S/SetLatch.md)
  - MyLatch (global variable)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md) (when PROCSIG_PARALLEL_APPLY_MESSAGE signal is received)

## Notes and Other Information
- This is a public function (non-static), callable from the signal handling infrastructure
- Designed specifically to be signal-safe - only performs minimal flag setting operations
- Part of PostgreSQL's asynchronous message handling architecture for parallel apply workers
- The actual message processing is deferred to HandleParallelApplyMessages() which runs outside signal context
- Works in conjunction with the CHECK_FOR_INTERRUPTS() mechanism to provide safe signal handling
- Critical for enabling leader apply workers to notify parallel workers of pending messages or errors

## Simplified Source

```c
// Simplified version of HandleParallelApplyMessageInterrupt
void HandleParallelApplyMessageInterrupt(void) {
    // Step 1: Mark that an interrupt is pending for CHECK_FOR_INTERRUPTS()
    InterruptPending = true;

    // Step 2: Mark that parallel apply message processing is needed
    ParallelApplyMessagePending = true;

    // Step 3: Wake up any waiting processes to handle the pending message
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added descriptive comments for each step
- Preserved the exact original logic (no actual simplification needed due to function's brevity)
- Emphasized the signal-safe nature and minimal operations
- Maintained the three-step process: interrupt flagging, message flagging, and process awakening