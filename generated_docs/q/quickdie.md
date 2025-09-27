# quickdie

## Location
[src/backend/tcop/postgres.c:2902-2998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2902-L2998)

## Overview
Signal handler function that performs immediate process termination when a SIGQUIT signal is received from the postmaster.

## Definition
```c
void quickdie(SIGNAL_ARGS)
```

## Detailed Description
This is a critical signal handler function in PostgreSQL that handles immediate shutdown scenarios. It is invoked when a SIGQUIT signal is received from the postmaster, which typically occurs in two main situations:
1. Another backend process has crashed ("bought the farm")
2. An immediate shutdown has been requested

The function performs emergency cleanup and termination with several important safety measures:
- Blocks further SIGQUIT signals to prevent nested calls
- Prevents interrupt processing to avoid downgrading quickdie to a query cancel
- Handles different quit scenarios (crash recovery, immediate stop, unexpected SIGQUIT)
- Attempts to notify the client about the termination reason
- Performs emergency exit without normal cleanup procedures

The function deliberately avoids normal cleanup procedures (proc_exit, atexit callbacks) because shared memory may be corrupted, making standard cleanup unsafe. It uses _exit(2) instead of _exit(0) to signal an abnormal termination to the postmaster.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard signal handler arguments (signal number, signal info, context)

## Dependencies
- Functions called/Symbols referenced:
  - sigaddset, sigprocmask (signal masking functions)
  - SIGQUIT (signal constant)
  - SIG_SETMASK (signal mask operation)
  - HOLD_INTERRUPTS (interrupt prevention macro)
  - DestRemote, DestNone (output destination constants)
  - [GetQuitSignalReason](../G/GetQuitSignalReason.md) (function to determine quit reason)
  - PMQUIT_NOT_SENT, PMQUIT_FOR_CRASH, PMQUIT_FOR_STOP (quit reason constants)
  - WARNING_CLIENT_ONLY (error reporting level)
  - ereport (error reporting function)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (in src/backend/tcop/postgres.c:4285) - registered as signal handler

## Notes and Other Information
- This is an async-signal-safe function designed to work reliably in signal context
- Uses emergency termination (_exit(2)) to avoid potentially corrupted cleanup code
- Implements defensive programming by clearing error context stack
- Attempts client notification despite signal handler constraints
- The function name "quickdie" reflects its immediate, no-cleanup termination behavior
- Exit code 2 triggers postmaster system reset cycle for manual SIGQUIT scenarios
- Critical for PostgreSQL's crash recovery and immediate shutdown mechanisms

## Simplified Source

```c
// Simplified version of quickdie
void quickdie(SIGNAL_ARGS) {
    // Prevent nested signal handler calls
    sigaddset(&BlockSig, SIGQUIT);
    sigprocmask(SIG_SETMASK, &BlockSig, NULL);

    // Prevent interrupts from downgrading quickdie to query cancel
    HOLD_INTERRUPTS();

    // Don't send output to client during authentication
    if (ClientAuthInProgress && whereToSendOutput == DestRemote) {
        whereToSendOutput = DestNone;
    }

    // Clear error context for safer signal handling
    error_context_stack = NULL;

    // Notify client about termination reason
    switch (GetQuitSignalReason()) {
        case PMQUIT_NOT_SENT:
            // Unexpected SIGQUIT signal
            ereport(WARNING, (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating connection because of unexpected SIGQUIT signal")));
            break;

        case PMQUIT_FOR_CRASH:
            // Another process crashed - need recovery
            ereport(WARNING_CLIENT_ONLY, (errcode(ERRCODE_CRASH_SHUTDOWN),
                    errmsg("terminating connection because of crash of another server process"),
                    errdetail("Rolling back transaction due to potential shared memory corruption")));
            break;

        case PMQUIT_FOR_STOP:
            // Immediate shutdown requested
            ereport(WARNING_CLIENT_ONLY, (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating connection due to immediate shutdown command")));
            break;
    }

    // Emergency exit without cleanup (shared memory may be corrupted)
    // Use exit code 2 to signal abnormal termination to postmaster
    _exit(2);
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic flow
- Consolidated error messages to focus on core information
- Abstracted complex signal handling details into clear comments
- Maintained the three-way switch logic for different quit scenarios
- Preserved critical safety measures (signal blocking, interrupt prevention)
- Kept emergency exit behavior that's essential to the function's purpose