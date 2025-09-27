# signal_child

## Location
[src/backend/postmaster/postmaster.c:3422-3451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3422-L3451)

## Overview
signal_child sends signals to PostgreSQL child processes and their process groups, handling race conditions and ensuring reliable signal delivery to both direct children and any spawned subprocesses.

## Definition
static void signal_child(pid_t pid, int signal)

## Detailed Description
signal_child is a robust utility function for sending signals to PostgreSQL child processes that addresses the complexities of process group signaling in Unix-like systems. The function first sends the signal directly to the specified child process. On systems that support setsid(), it also sends the signal to the entire process group (negative PID) for certain critical signals (SIGINT, SIGTERM, SIGQUIT, SIGKILL, SIGABRT). This dual-signaling approach ensures that subprocesses spawned by the child (such as archive scripts or system() calls) also receive the signal. The function handles race conditions where recently-forked children might not have executed setsid() yet by signaling both the child directly and the process group. It assumes that signaling twice will not cause problems and that the child will handle signals before spawning grandchildren. Debug logging is provided for failed kill() calls to aid in troubleshooting.

## Parameters / Member Variables
- : Process ID of the child process to signal
- : Signal number to send (e.g., SIGTERM, SIGINT, SIGQUIT, SIGKILL, SIGABRT)

## Dependencies
- Functions called/Symbols referenced:
  - kill
  - elog
  - DEBUG3
  - HAVE_SETSID (preprocessor condition)
  - SIGINT, SIGTERM, SIGQUIT, SIGKILL, SIGABRT (signal constants)
- Called from (representative examples):
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)
  - [process_pm_reload_request](../p/process_pm_reload_request.md)
  - [processCancelRequest](../p/processCancelRequest.md)
  - [sigquit_child](sigquit_child.md)
  - [SignalSomeChildren](../S/SignalSomeChildren.md)
  - [TerminateChildren](../T/TerminateChildren.md)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md)

## Notes and Other Information
- Implements process group signaling to handle subprocesses spawned by PostgreSQL children
- Uses conditional compilation with HAVE_SETSID to support systems without setsid()
- Only sends process group signals for "generally interpreted" signals that should affect subprocesses
- Critical for ensuring reliable shutdown of archive scripts, background workers, and user scripts
- Provides debug logging for signal delivery failures to aid in system troubleshooting
- Assumes that duplicate signal delivery is safe and handled properly by receiving processes

## Simplified Source

```c
// Simplified version of signal_child
static void signal_child(pid_t pid, int signal) {
    // Step 1: Send signal directly to the child process
    if (kill(pid, signal) < 0) {
        elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) pid, signal);
    }

    // Step 2: For critical signals, also signal the process group
    // This ensures subprocesses (scripts, system() calls) also receive the signal
    if (signal == SIGINT || signal == SIGTERM || signal == SIGQUIT ||
        signal == SIGKILL || signal == SIGABRT) {

        // Send to process group (negative PID)
        if (kill(-pid, signal) < 0) {
            elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) (-pid), signal);
        }
    }
}
```

Key simplifications made:
- Removed HAVE_SETSID conditional compilation for clarity
- Consolidated switch statement into simple if condition
- Added clear comments explaining the two-step signaling approach
- Focused on the main logic: direct child signaling + process group signaling
- Preserved error handling and debug logging
- Maintained the essential dual-signaling strategy for handling race conditions