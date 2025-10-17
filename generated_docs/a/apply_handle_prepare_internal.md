# apply_handle_prepare_internal

## Location
[src/backend/replication/logical/worker.c:1073-1109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1073-L1109)

## Overview
apply_handle_prepare_internal is a common function that handles the internal preparation of two-phase transactions by generating unique GIDs and executing the prepare phase in PostgreSQL logical replication.

## Definition
```c
static void apply_handle_prepare_internal(LogicalRepPreparedTxnData *prepare_data)
```

## Detailed Description
This function performs the core work of preparing a two-phase transaction during logical replication. It generates a unique Global Transaction Identifier (GID) using the subscription OID and transaction ID to avoid conflicts when multiple subscriptions exist from the same node. The function ensures proper transaction block management by starting a transaction block if one is not already active, updates the replication origin state with LSN and timestamp information for crash recovery purposes, and finally calls PrepareTransactionBlock to complete the prepare phase of the two-phase commit protocol.

## Parameters / Member Variables
- `prepare_data`: Pointer to LogicalRepPreparedTxnData structure containing prepared transaction information including transaction ID, end LSN, and prepare timestamp

## Dependencies
- Functions called/Symbols referenced:
  - GIDSIZE (constant defining maximum GID size)
  - [TwoPhaseTransactionGid](../T/TwoPhaseTransactionGid.md) (generates unique GID for two-phase transactions)
  - [IsTransactionBlock](../I/IsTransactionBlock.md) (checks if currently in a transaction block)
  - [BeginTransactionBlock](../B/BeginTransactionBlock.md) (starts a new transaction block)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (commits the begin command)
  - [PrepareTransactionBlock](../P/PrepareTransactionBlock.md) (performs the actual prepare operation)
- Called from:
  - [apply_handle_prepare](apply_handle_prepare.md) (handles PREPARE messages)
  - [apply_handle_stream_prepare](apply_handle_stream_prepare.md) (handles streaming prepare messages)

## Notes and Other Information
- Generates unique GIDs using subscription OID and transaction XID to prevent deadlocks with multiple subscriptions
- Contains detailed comments explaining the rationale for custom GID generation instead of using server-provided GIDs
- Updates replication origin session state for proper crash recovery positioning
- Ensures proper transaction block balancing by starting a block if none exists
- Part of PostgreSQL's two-phase commit support in logical replication
- The function is static and used internally by multiple prepare handling functions
- Critical for maintaining consistency in two-phase commit scenarios across logical replication

## Simplified Source

```c
static void
apply_handle_prepare_internal(LogicalRepPreparedTxnData *prepare_data)
{
    char gid[GIDSIZE];

    // Generate unique GID using subscription OID and transaction XID
    // This prevents deadlocks when multiple subscriptions exist
    TwoPhaseTransactionGid(MySubscription->oid, prepare_data->xid,
                           gid, sizeof(gid));

    // Ensure we have a transaction block for proper balancing
    if (!IsTransactionBlock()) {
        BeginTransactionBlock();
        CommitTransactionCommand(); // Complete the Begin command
    }

    // Update origin state for crash recovery positioning
    replorigin_session_origin_lsn = prepare_data->end_lsn;
    replorigin_session_origin_timestamp = prepare_data->prepare_time;

    // Execute the prepare operation
    PrepareTransactionBlock(gid);
}
```