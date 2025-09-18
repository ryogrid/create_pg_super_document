# LogicalRepSyncTableStart

## Location
src/backend/replication/logical/tablesync.c: 1309 - 1597

## Overview
LogicalRepSyncTableStart orchestrates the initial table synchronization phase for logical replication, establishing replication slots, performing data copying, and setting up origin tracking to ensure consistent table state replication.

## Definition
```c
static char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos)
```

## Detailed Description
This is the core function responsible for initiating and managing table synchronization in PostgreSQL's logical replication system. It handles the complex process of synchronizing a single table from the publisher to the subscriber, ensuring data consistency and proper state management throughout the operation.

The function performs several critical phases:

1. **State Assessment**: Checks the current synchronization state and determines if work needs to be done
2. **Connection Management**: Establishes a dedicated WAL receiver connection to the publisher
3. **Slot Creation**: Creates a temporary replication slot for consistent data capture during copy
4. **Origin Tracking Setup**: Establishes replication origin tracking for crash recovery and progress monitoring
5. **Permission Validation**: Ensures proper access controls and RLS compliance
6. **Data Copy**: Performs the initial COPY operation to transfer existing table data
7. **State Transitions**: Updates synchronization state through various phases (DATASYNC, FINISHEDCOPY, SYNCWAIT)

The function implements sophisticated error handling and recovery mechanisms, including cleanup of partially created replication slots and proper transaction boundary management.

## Parameters / Member Variables
- `origin_startpos`: Output parameter that receives the LSN position from which to start logical replication after the initial copy phase

## Dependencies
- Functions called/Symbols referenced:
  - StartTransactionCommand/CommitTransactionCommand
  - GetSubscriptionRelState
  - ReplicationSlotNameForTablesync
  - walrcv_connect
  - ReplicationSlotDropAtPubNode
  - UpdateSubscriptionRelState
  - walrcv_create_slot
  - replorigin_create/replorigin_session_setup
  - copy_table
  - wait_for_worker_state_change
- Called from (representative examples):
  - start_table_sync

## Notes and Other Information
- The function uses REPEATABLE READ isolation level on the publisher to ensure consistency between slot creation and data copy
- Implements comprehensive permission checking including ACL verification and Row Level Security (RLS) compliance
- Supports both superuser and non-superuser subscription owners with appropriate privilege switching
- Creates permanent replication slots that persist beyond the initial copy phase for ongoing synchronization
- The returned slot name is palloc'd and must be managed by the caller
- Handles recovery scenarios where previous synchronization attempts were interrupted
- Integrates with PostgreSQL's transaction system and statistics reporting mechanisms