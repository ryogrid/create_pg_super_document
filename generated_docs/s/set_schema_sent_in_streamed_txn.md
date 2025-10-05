# set_schema_sent_in_streamed_txn

## Location
[src/backend/replication/pgoutput/pgoutput.c:1981-2001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1981-L2001)

## Overview
This function marks that schema information has been sent for a specific relation within a given streamed transaction by adding the transaction ID to the relation's tracking list.

## Definition
```c
static void
set_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid)
```

## Detailed Description
The `set_schema_sent_in_streamed_txn` function is used in PostgreSQL's logical replication system to record that schema information for a particular relation has been transmitted to downstream subscribers within a specific streamed transaction. This function updates the relation's synchronization entry by adding the transaction ID to its list of streamed transactions. The function carefully manages memory contexts to ensure the transaction ID list is allocated in the appropriate long-lived cache memory context.

## Parameters / Member Variables
- `entry`: RelationSyncEntry pointer containing the relation's synchronization metadata to be updated
- `xid`: TransactionId representing the transaction ID to add to the streamed transactions list

## Dependencies
- Functions called/Symbols referenced:
  - [lappend_xid](../l/lappend_xid.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (implicit via CacheMemoryContext)
- Called from (representative examples):
  - [maybe_send_schema](../m/maybe_send_schema.md)

## Notes and Other Information
- The function switches to CacheMemoryContext before modifying the list to ensure proper memory allocation lifetime
- Uses lappend_xid to add the transaction ID to the existing list in the RelationSyncEntry
- Properly restores the previous memory context after the operation
- Works in conjunction with get_schema_sent_in_streamed_txn to implement schema transmission tracking
- This tracking prevents redundant schema transmissions within the same streamed transaction
- The list is maintained in cache memory to persist across multiple operations within a decoding session
- Essential for optimizing logical replication performance by avoiding unnecessary network traffic
- The memory context switch ensures the transaction ID list survives beyond the current function call

## Simplified Source

```c
static void
set_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid) {
    // Switch to cache memory context for persistent allocation
    MemoryContext oldctx = MemoryContextSwitchTo(CacheMemoryContext);

    // Add transaction ID to the streamed transactions list
    entry->streamed_txns = lappend_xid(entry->streamed_txns, xid);

    // Restore previous memory context
    MemoryContextSwitchTo(oldctx);
}
```