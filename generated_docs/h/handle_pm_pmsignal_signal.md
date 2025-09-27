# handle_pm_pmsignal_signal

## Location
[src/backend/postmaster/postmaster.c:2076-2085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2076-L2085)

## Overview
Signal handler that processes SIGUSR1 signals from child processes and pg_ctl to notify the postmaster of pending 'pmsignals'.

## Definition
```c
static void handle_pm_pmsignal_signal(SIGNAL_ARGS)
```

## Detailed Description
handle_pm_pmsignal_signal is a signal handler function that responds to SIGUSR1 signals sent to the postmaster. This signal serves dual purposes:

1. **Child Process Communication**: Child processes use SIGUSR1 to notify the postmaster of 'pmsignals' - internal communication messages that require the postmaster's attention
2. **External Tool Communication**: pg_ctl uses SIGUSR1 to request the postmaster to check for logrotate and promote files

The handler sets a global flag (pending_pm_pmsignal) and wakes up the postmaster's main event loop by setting its latch. This ensures the postmaster will process the pending signals in its main loop rather than handling them directly in the signal handler context, which is safer and more reliable.

The function follows PostgreSQL's pattern of minimal signal handlers that defer actual work to the main event loop.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md)
  - SIGNAL_ARGS (macro)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (as signal handler registration)

## Notes and Other Information
- Static function - only accessible within postmaster.c
- Uses async-signal-safe operations only (setting boolean flag and latch)
- Part of PostgreSQL's inter-process communication system
- Critical for postmaster responsiveness to child process events
- The actual signal processing happens later in the main event loop, not in the handler itself
- Registered as SIGUSR1 handler during PostmasterMain initialization
- Essential for operations like log rotation and standby promotion triggered by pg_ctl

## Simplified Source

```c
// Simplified version of handle_pm_pmsignal_signal
static void handle_pm_pmsignal_signal(SIGNAL_ARGS) {
    // Step 1: Mark that a pmsignal is pending
    pending_pm_pmsignal = true;

    // Step 2: Wake up the postmaster's main event loop
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Function is already very simple, preserving original structure
- Added descriptive comments explaining the two core operations
- Maintained the async-signal-safe pattern (minimal work in signal handler)
- Focuses on the essential function: flagging pending work and waking the main loop