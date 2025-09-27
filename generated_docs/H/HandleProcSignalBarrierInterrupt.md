# HandleProcSignalBarrierInterrupt

## Location
[src/backend/storage/ipc/procsignal.c:448-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L448-L463)

## Overview
Handles receipt of an interrupt indicating a global barrier event by setting flags to defer the actual processing work to a safe context.

## Definition
```c
static void HandleProcSignalBarrierInterrupt(void)
```

## Detailed Description
HandleProcSignalBarrierInterrupt is a lightweight signal handler that responds to barrier-related interrupts (PROCSIG_BARRIER signals). The function operates under the constraints of signal handler safety by:

1. **Interrupt Flag Setting**: Sets the global InterruptPending flag to indicate that interrupt processing is needed.

2. **Barrier Flag Setting**: Sets the ProcSignalBarrierPending flag to specifically indicate that barrier processing is required.

3. **Deferred Processing**: Defers all actual barrier work to ProcessProcSignalBarrier(), which runs in a safer execution context outside the signal handler.

The function is deliberately minimal because signal handlers have severe restrictions on what operations are safe to perform. Specifically, it avoids accessing the barrier generation counter because 64-bit atomic operations might use spinlock-based emulation that is not signal-safe.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (only sets global variables)
- Global variables accessed:
  - InterruptPending (global interrupt flag)
  - ProcSignalBarrierPending (barrier-specific interrupt flag)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md) (main SIGUSR1 signal dispatcher)

## Notes and Other Information
- Declared as static, making it internal to the procsignal.c module
- Designed for signal handler safety - performs minimal work
- The latch is set by the calling procsignal_sigusr1_handler, not by this function
- All substantive barrier processing is deferred to ProcessProcSignalBarrier()
- Avoids accessing 64-bit atomics due to potential spinlock emulation in signal context
- Part of the two-phase barrier interrupt handling design for safety
- Located in src/backend/storage/ipc/procsignal.c:448-463

## Simplified Source

```c
// Simplified version of HandleProcSignalBarrierInterrupt
static void HandleProcSignalBarrierInterrupt(void) {
    // Set global interrupt flag to indicate processing needed
    InterruptPending = true;

    // Set barrier-specific flag for deferred processing
    ProcSignalBarrierPending = true;

    // Note: latch will be set by procsignal_sigusr1_handler
}
```

Key simplifications made:
- Added descriptive comments for each flag setting
- Removed the detailed function comment (preserved in overview)
- Focused on the core signal handler logic
- Maintained the essential safety-first design pattern