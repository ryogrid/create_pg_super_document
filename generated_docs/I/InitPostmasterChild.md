# InitPostmasterChild

## Location
[src/backend/utils/init/miscinit.c:96-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L96-L181)

## Overview
Initializes the basic environment for a postmaster child process, setting up essential process state and signal handling mechanisms that all PostgreSQL child processes require.

## Definition
```c
void InitPostmasterChild(void)
```

## Detailed Description
InitPostmasterChild performs critical initialization steps that transform a newly forked/spawned child process into a proper PostgreSQL subprocess. This function establishes the fundamental runtime environment that all postmaster children need, including signal handling, process identification, latch support, and platform-specific setup.

The function sets the IsUnderPostmaster flag to indicate this process is now a subprocess, initializes platform-specific signal handling (especially on Windows), establishes stack depth checking, and sets up process-local latch support for inter-process communication. It also configures signal handlers, particularly for SIGQUIT to ensure proper crash handling, and establishes monitoring for postmaster death.

This initialization is designed to be called as early as possible in a child process's lifecycle, after basic variable setup but before any substantial work begins.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_signal_initialize](../p/pgwin32_signal_initialize.md) (Windows signal handling)
  - set_stack_base (stack depth checking setup)
  - [InitProcessGlobals](InitProcessGlobals.md) (global process state)
  - [on_exit_reset](../o/on_exit_reset.md) (clear parent's exit handlers)
  - [pqinitmask](../p/pqinitmask.md) (signal mask initialization for EXEC_BACKEND)
  - InitializeLatchSupport (latch infrastructure)
  - [InitProcessLocalLatch](InitProcessLocalLatch.md) (process-local latch)
  - InitializeLatchWaitSet (wait event infrastructure)
  - [pqsignal](../p/pqsignal.md) (signal handler setup)
  - [SignalHandlerForCrashExit](../S/SignalHandlerForCrashExit.md) (crash signal handler)
  - PostmasterDeathSignalInit (postmaster monitoring)

- Called from (representative examples):
  - [postmaster_child_launch](../p/postmaster_child_launch.md)
  - [SubPostmasterMain](../S/SubPostmasterMain.md)

## Notes and Other Information
- Must be called after read_backend_variables() on EXEC_BACKEND builds
- Sets up binary mode for stderr on Windows to maintain syslogger pipe protocol
- Attempts to make the process a group leader for consistent signal propagation
- Removes SIGQUIT from the blocked signal set to ensure responsive crash handling
- Establishes file descriptor monitoring for postmaster death detection
- Critical for proper subprocess initialization in PostgreSQL's process model