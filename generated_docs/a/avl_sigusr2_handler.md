# avl_sigusr2_handler

## Location
src/backend/postmaster/autovacuum.c: 1344 - 1358

## Overview
A signal handler for SIGUSR2 in the autovacuum launcher that responds to worker lifecycle events including startup, completion, and fork failures.

## Definition


## Detailed Description
The  function serves as the SIGUSR2 signal handler for the autovacuum launcher process. This signal is used by the postmaster to notify the launcher about various worker lifecycle events:

1. **Worker startup**: When a new autovacuum worker process has been successfully created and is running
2. **Worker completion**: When an autovacuum worker has finished its work and terminated
3. **Fork failure**: When the postmaster failed to create a worker process (usually due to system resource constraints)

The handler implementation is deliberately minimal and signal-safe. It simply sets the global flag  to  and uses  to wake up the launcher's main loop. The actual processing of the signal condition is handled in the main loop where it's safe to perform complex operations.

This design follows PostgreSQL's standard pattern for signal handling: keep the signal handler simple and defer complex work to the main event loop.

## Parameters / Member Variables
- : Standard PostgreSQL signal handler arguments macro

## Dependencies
- Functions called/Symbols referenced:
  -  (wake up the launcher main loop)
  -  (the launcher's latch for synchronization)
  -  (global flag to indicate signal received)

- Called from (representative examples):
  - Signal delivery mechanism when registered via  (src/backend/postmaster/autovacuum.c:399)

## Notes and Other Information
- Signal-safe implementation that defers complex processing to the main loop
- Part of the inter-process communication mechanism between postmaster and autovacuum launcher
- The  flag is checked and cleared in the launcher's main loop
- Critical for maintaining synchronization between worker allocation and actual worker processes
- Enables prompt response to worker lifecycle changes without polling
- Follows PostgreSQL's standard signal handling patterns for reliability and safety