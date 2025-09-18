# signal_child

## Location
src/backend/postmaster/postmaster.c: 3422 - 3451

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
  - PostmasterStateMachine
  - process_pm_reload_request
  - processCancelRequest
  - sigquit_child
  - SignalSomeChildren
  - TerminateChildren
  - process_pm_pmsignal

## Notes and Other Information
- Implements process group signaling to handle subprocesses spawned by PostgreSQL children
- Uses conditional compilation with HAVE_SETSID to support systems without setsid()
- Only sends process group signals for "generally interpreted" signals that should affect subprocesses
- Critical for ensuring reliable shutdown of archive scripts, background workers, and user scripts
- Provides debug logging for signal delivery failures to aid in system troubleshooting
- Assumes that duplicate signal delivery is safe and handled properly by receiving processes