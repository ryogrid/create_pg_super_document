# ReplSlotSyncWorkerMain

## Location
src/backend/replication/logical/slotsync.c: 1331 - 1509

## Overview
The main entry point and loop for PostgreSQL's slot synchronization worker process, responsible for establishing a connection to the primary server and continuously synchronizing logical replication slots.

## Definition


## Detailed Description
This function serves as the main entry point for the slot synchronization worker process in PostgreSQL's logical replication system. It initializes the worker process, establishes a connection to the primary server, and runs an infinite loop to continuously synchronize logical failover slots. The function handles process setup including signal handling, database connection, validation of remote server configuration, and proper cleanup on exit. It operates as a background worker that ensures standby servers maintain up-to-date copies of logical replication slots from the primary server.

## Parameters / Member Variables
- : Startup data passed to the worker (currently unused, expected to be NULL)
- : Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - InitProcess (process initialization)
  - [BaseInit](../B/BaseInit.md) (early backend initialization)
  - [InitPostgres](../I/InitPostgres.md) (database connection initialization)
  - walrcv_connect (primary server connection)
  - synchronize_slots (core slot synchronization logic)
  - [ProcessSlotSyncInterrupts](../P/ProcessSlotSyncInterrupts.md) (interrupt handling)
  - [wait_for_slot_activity](../w/wait_for_slot_activity.md) (timing control)
  - validate_remote_info (primary server validation)
  - [check_and_set_sync_info](../c/check_and_set_sync_info.md) (sync context setup)
  - SetProcessingMode (processing state management)
  - Various signal handlers and cleanup functions

- Called from (representative examples):
  - child_process_kind (src/backend/postmaster/launch_backend.c:198)
  - Referenced in SLOTSYNC_H header (src/include/replication/slotsync.h:29)

## Notes and Other Information
- This is the main function for B_SLOTSYNC_WORKER backend type
- Implements comprehensive error handling with sigsetjmp/siglongjmp
- Establishes secure database connection as superuser for slot operations
- Runs indefinitely until terminated by SIGINT or error conditions
- Sets up proper signal handling for graceful shutdown and configuration reload
- Registers cleanup callbacks for proper resource management on exit
- Validates that the server is not a cascading standby before proceeding
- Uses walreceiver infrastructure for primary server communication
- Critical component of PostgreSQL's logical replication failover capability