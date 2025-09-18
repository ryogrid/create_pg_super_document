# pgarch_waken_stop

## Location
src/backend/postmaster/pgarch.c: 297 - 309

## Overview
A SIGUSR2 signal handler for the PostgreSQL archiver process that initiates graceful shutdown by setting a termination flag and waking the main loop.

## Definition
static void pgarch_waken_stop(SIGNAL_ARGS)

## Detailed Description
pgarch_waken_stop serves as the signal handler for SIGUSR2 in the PostgreSQL archiver process. When invoked, it sets the ready_to_stop global flag to true, indicating that the archiver should perform one final archiving cycle before shutting down gracefully. The function then calls SetLatch() on MyLatch to wake up the main archiver loop that may be waiting, ensuring the shutdown signal is processed promptly.

This function is part of PostgreSQL's graceful shutdown mechanism for the archiver background process, allowing the system to cleanly terminate archiving operations without losing data or leaving the archive in an inconsistent state.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro which provides standard signal handler parameters (signal number, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SetLatch (to wake up the main loop)
- Called from (representative examples):
  - PgArchiverMain (registered as SIGUSR2 handler)

## Notes and Other Information
- This is a static function internal to the pgarch.c module
- The function is signal-safe and performs minimal operations to avoid race conditions
- The ready_to_stop flag is checked by the main archiver loop to determine when to exit
- Part of PostgreSQL's background process management infrastructure