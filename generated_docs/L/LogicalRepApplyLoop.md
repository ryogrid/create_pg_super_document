# LogicalRepApplyLoop

## Location
src/backend/replication/logical/worker.c: 3491 - 3754

## Overview
LogicalRepApplyLoop is the main event loop that handles receiving, processing, and applying logical replication messages from a publisher in PostgreSQL's logical replication system.

## Definition


## Detailed Description
This function implements the core message processing loop for a logical replication apply worker. It continuously receives messages from the publisher via the WAL receiver connection, processes different message types ('w' for WAL data, 'k' for keepalive), applies changes to the local database, and sends feedback to the publisher. The function manages memory contexts, handles timeouts, processes configuration reloads, and maintains replication statistics. It operates in an infinite loop until the stream ends, handling both streamed and non-streamed transactions while managing error contexts and ensuring proper cleanup.

## Parameters / Member Variables
- : The LSN (Log Sequence Number) of the last successfully received and processed message from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - AllocSetContextCreate
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - walrcv_receive
  - [apply_dispatch](../a/apply_dispatch.md)
  - [UpdateWorkerStats](../U/UpdateWorkerStats.md)
  - [send_feedback](../s/send_feedback.md)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md)
  - [process_syncing_tables](../p/process_syncing_tables.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - walrcv_endstreaming
- Called from (representative examples):
  - [start_apply](../s/start_apply.md)

## Notes and Other Information
- This is a static function internal to the worker.c file
- Creates and manages two memory contexts: ApplyMessageContext (reset after each message) and LogicalStreamingContext (for streaming mode)
- Processes two main message types: 'w' (WAL data) and 'k' (keepalive messages)
- Implements timeout handling for wal_receiver_timeout to detect connection issues
- Sends periodic feedback messages to the publisher to acknowledge progress
- Handles configuration reloads via SIGHUP signal processing
- Manages table synchronization when not in active transactions
- Uses error context callbacks for detailed error reporting during message processing
- Exits cleanly when the publisher ends the data stream