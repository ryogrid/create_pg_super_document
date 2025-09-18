# ReplicationSlotDropAtPubNode

## Location
src/backend/commands/subscriptioncmds.c: 1844 - 1898

## Overview
ReplicationSlotDropAtPubNode drops a replication slot on the publisher node through an established replication connection, with support for graceful error handling.

## Definition


## Detailed Description
ReplicationSlotDropAtPubNode executes a DROP_REPLICATION_SLOT command on the remote publisher node via the established WAL receiver connection. This function is a critical component of subscription cleanup and maintenance operations.

The function constructs and executes a DROP_REPLICATION_SLOT SQL command with the WAIT option, which ensures the operation completes before returning. It handles three possible outcomes:
1. Success (WALRCV_OK_COMMAND): Reports success via NOTICE message
2. Slot not found with missing_ok=true: Logs the issue but doesn't fail 
3. Any other error: Reports ERROR and fails the operation

The function uses PG_TRY/PG_FINALLY blocks to ensure proper memory cleanup of the command string regardless of execution outcome. This is essential for preventing memory leaks during error conditions.

## Parameters / Member Variables
- : Active WAL receiver connection to the publisher node where the slot exists
- : Name of the replication slot to be dropped on the publisher
- : If true, treat missing slot as non-fatal and log instead of erroring

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_exec: Executes SQL command on the remote publisher via replication connection
  - quote_identifier: Safely quotes the slot name to prevent SQL injection
  - walrcv_clear_result: Cleans up result structure after command execution  
  - load_file: Loads libpqwalreceiver library for WAL receiver functionality
- Called from (representative examples):
  - DropSubscription: During subscription cleanup in subscriptioncmds.c:1814, 1825
  - process_syncing_tables_for_sync: During tablesync cleanup in tablesync.c:345
  - LogicalRepSyncTableStart: During tablesync initialization in tablesync.c:1396

## Notes and Other Information
- Uses the WAIT option to ensure synchronous completion of the slot drop operation
- Provides different error reporting levels based on the missing_ok parameter for flexible error handling
- Essential for proper cleanup during subscription drops and tablesync operations
- Memory management handled via PG_TRY/PG_FINALLY to prevent leaks on errors
- Requires an active replication connection to function - does not establish connections itself
- Part of the logical replication infrastructure for managing publisher-side resources from subscribers