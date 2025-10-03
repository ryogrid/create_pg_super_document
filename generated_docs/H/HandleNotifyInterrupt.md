# HandleNotifyInterrupt

## Location
[src/backend/commands/async.c:1804-1833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1804-L1833)

## Overview
Signal handler function that sets flags to indicate a pending NOTIFY interrupt needs to be processed, safely deferring the actual notification processing to outside the signal handler context.

## Definition
```c
void HandleNotifyInterrupt(void)
```

## Detailed Description
This function serves as the signal handler portion of PostgreSQL's asynchronous notification interrupt handling system. It is designed to be called from within a signal handler context (specifically SIGUSR1), which imposes strict limitations on what operations can be safely performed.

The function performs two critical but minimal operations:
1. Sets the global flag `notifyInterruptPending` to true, indicating that notification processing work is required
2. Calls SetLatch(MyLatch) to ensure the backend's event loop will wake up and process the pending interrupt

The actual notification processing is deliberately deferred to ProcessNotifyInterrupt(), which will be called later from a safe context when the backend processes the latch signal. This design pattern ensures signal safety while maintaining responsiveness to incoming notifications.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md)

## Notes and Other Information
- **Signal Safety**: Explicitly designed to be called from a signal handler context with severe restrictions on allowable operations
- **Deferred Processing**: Only sets flags and signals - actual notification work happens later in ProcessNotifyInterrupt()
- **Latch Mechanism**: Uses PostgreSQL's latch system to ensure the backend's main loop will process the pending interrupt
- Part of the inter-process communication system for LISTEN/NOTIFY functionality
- Located in src/backend/commands/async.c:1804-1833

## Simplified Source

```c
// Simplified version of HandleNotifyInterrupt
void HandleNotifyInterrupt(void) {
    // Step 1: Mark that notification processing is needed
    notifyInterruptPending = true;

    // Step 2: Wake up the backend's main event loop
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Removed detailed signal handler safety comments for clarity
- Focused on the two core operations: flag setting and latch signaling
- Emphasized the minimal, signal-safe nature of the function
- Preserved the essential deferred-processing pattern