# PMState

## Location
src/backend/postmaster/postmaster.c: 329 - 345

## Overview
PMState is an enumeration that represents the different operational states of the PostgreSQL postmaster process throughout its lifecycle.

## Definition


## Detailed Description
PMState tracks the postmaster's operational lifecycle from initialization through normal operation to shutdown. This state machine is critical for coordinating the various child processes and ensuring proper startup, recovery, and shutdown sequences. The postmaster uses these states to determine which operations are allowed, which child processes should be running, and how to handle various signals and events.

The state transitions guide the postmaster through crash recovery, archive recovery, hot standby operations, and clean shutdown procedures. Each state has specific behaviors regarding new connections, child process management, and response to shutdown signals.

## Parameters / Member Variables
- : Initial state during postmaster startup and initialization
- : Waiting for the startup process to complete database recovery and initialization
- : In archive recovery mode, replaying WAL from archives
- : In hot standby mode, allowing read-only queries during recovery
- : Normal operational state where the database accepts read-write connections
- : Shutdown initiated, stopping remaining backend processes
- : Waiting for all live backend processes to exit cleanly
- : Waiting for checkpointer to complete shutdown checkpoint
- : Final shutdown phase, waiting for archiver and WAL senders to finish
- : Waiting for dead-end children to exit during shutdown
- : All important child processes have exited, ready for final termination

## Dependencies
- Functions called/Symbols referenced:
  - PM_INIT (initialization state)
  - Various state-specific process management functions
- Called from (representative examples):
  - ServerLoop (main postmaster event loop)
  - Signal handlers for shutdown coordination
  - Child process management routines
  - Recovery and startup coordination functions

## Notes and Other Information
- The global variable pmState tracks the current postmaster state, initialized to PM_INIT
- State transitions are carefully coordinated with child process lifecycle events
- The connsAllowed variable provides additional connection control during smart shutdown
- Critical for proper crash recovery, hot standby operations, and clean shutdown procedures
- Different states allow or restrict new client connections based on database readiness
- Used extensively in signal handling to determine appropriate responses to shutdown requests