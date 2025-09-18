# ProcessWalRcvInterrupts

## Location
src/backend/replication/walreceiver.c: 162 - 182

## Overview
ProcessWalRcvInterrupts handles interrupts for the WAL receiver process, specifically processing shutdown requests that arrive via SIGTERM signals.

## Definition
void ProcessWalRcvInterrupts(void)

## Detailed Description
This function processes any interrupts that the WAL receiver process may have received and should be called whenever the process's latch has become set. The primary purpose is to handle SIGTERM signals safely without interrupting critical operations.

The function uses a two-phase interrupt handling approach: when SIGTERM arrives, the signal handler sets a flag variable (ShutdownRequestPending) and the process latch, rather than calling exit() directly. This prevents interruption during critical operations like holding spinlocks. The function checks this flag and terminates the process gracefully if a shutdown has been requested.

The function also calls CHECK_FOR_INTERRUPTS() to ensure proper signal reception on Windows platforms and to process any barrier events that may be pending.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - ShutdownRequestPending (global variable)
  - ereport
  - FATAL
  - errcode
  - ERRCODE_ADMIN_SHUTDOWN
  - errmsg

- Called from (representative examples):
  - libpqrcv_connect
  - libpqrcv_PQgetResult
  - libpqrcv_processTuples
  - WalReceiverMain
  - WalRcvWaitForStartPosition
  - walrcv_clear_result

## Notes and Other Information
- The function is designed to be called from any location where the WAL receiver process might block for extended periods
- Critical for safe shutdown handling in replication scenarios
- Part of PostgreSQL's streaming replication infrastructure
- The latch-based approach ensures that long-running operations can be interrupted safely