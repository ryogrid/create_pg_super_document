# TerminateChildren

## Location
[src/backend/postmaster/postmaster.c:3510-3544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3510-L3544)

## Overview
Sends a termination signal to all PostgreSQL child processes except the syslogger and dead_end backends, including both regular backends and special auxiliary processes.

## Definition
static void TerminateChildren(int signal)

## Detailed Description
This function provides comprehensive process termination capability by signaling all child processes managed by the postmaster. It first calls SignalChildren() to handle regular backend processes, then individually signals each auxiliary process if they are running (PID != 0). Special handling is provided for the startup process where certain signals (SIGQUIT, SIGKILL, SIGABRT) cause the StartupStatus to be set to STARTUP_SIGNALED. The function systematically covers all major PostgreSQL auxiliary processes including background writer, checkpointer, WAL writer, WAL receiver, WAL summarizer, autovacuum launcher, archiver, and slot sync worker.

## Parameters / Member Variables
- `signal`: The signal number to send to all child processes (e.g., SIGTERM, SIGQUIT, SIGKILL)

## Dependencies
- Functions called/Symbols referenced:
  - SignalChildren (signals regular backends)
  - [signal_child](../s/signal_child.md) (sends signal to individual processes)
  - Various global PID variables (StartupPID, BgWriterPID, etc.)
  - Signal constants (SIGQUIT, SIGKILL, SIGABRT)
  - STARTUP_SIGNALED status constant
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster loop)
  - [process_pm_shutdown_request](../p/process_pm_shutdown_request.md) (shutdown handling)
  - [process_pm_child_exit](../p/process_pm_child_exit.md) (child exit processing)

## Notes and Other Information
- Static function internal to postmaster.c
- Excludes syslogger process from termination as it needs to continue logging during shutdown
- Dead_end backends are excluded as they are already in cleanup state
- StartupStatus tracking allows the postmaster to know when startup process has been signaled
- Part of PostgreSQL's graceful and emergency shutdown procedures
- Each auxiliary process is checked individually to avoid signaling non-existent processes
- Used during normal shutdown, crash recovery, and emergency termination scenarios