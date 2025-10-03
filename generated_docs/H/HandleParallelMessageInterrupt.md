# HandleParallelMessageInterrupt

## Location
[src/backend/access/transam/parallel.c:1033-1043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1033-L1043)

## Overview
Handles receipt of an interrupt indicating a parallel worker message by setting flags and latches to trigger message processing during the next interrupt check.

## Definition

```c
void
HandleParallelMessageInterrupt(void)
```
## Detailed Description
This function is a signal handler that responds to interrupts indicating that parallel worker messages are available for processing. Since it operates within a signal handler context, it has severe restrictions on what operations it can safely perform. The function's primary responsibility is to set appropriate flags that will cause the main execution thread to process parallel messages during its next CHECK_FOR_INTERRUPTS() call.

The function sets two critical flags:
- : A general flag indicating that an interrupt requires processing
- : A specific flag indicating that parallel worker messages are waiting

It also sets the process latch () to wake up any code that might be waiting on it, ensuring prompt handling of the pending messages.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md)
  - IsParallelWorker (referenced in header)

## Notes and Other Information
- This function must be signal-safe as it operates within a signal handler context
- The actual message processing is deferred to HandleParallelMessages() which is called later during CHECK_FOR_INTERRUPTS()
- The function works in conjunction with the parallel worker communication system
- Setting MyLatch ensures that processes waiting on latches are awakened to handle the pending messages promptly

## Simplified Source

```c
// Simplified version of HandleParallelMessageInterrupt
void HandleParallelMessageInterrupt(void) {
    // Step 1: Signal that an interrupt needs processing
    InterruptPending = true;

    // Step 2: Mark that parallel messages are waiting
    ParallelMessagePending = true;

    // Step 3: Wake up any waiting processes
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added step-by-step comments explaining the purpose of each flag
- Preserved the complete original logic as the function is already minimal
- Emphasized the three-step process: general interrupt flag, specific parallel message flag, and latch notification