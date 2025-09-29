# ReorderBufferGetTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:431-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L431-L454)

## Overview
Allocates and initializes a new ReorderBufferTXN structure from the reorder buffer's transaction memory context.

## Definition

```c
static ReorderBufferTXN *
ReorderBufferGetTXN(ReorderBuffer *rb)
```
## Detailed Description
ReorderBufferGetTXN creates a fresh ReorderBufferTXN instance by allocating memory from the reorder buffer's specialized transaction context (txn_context). It initializes the transaction structure with default values, setting up empty doubly-linked lists for changes, tuple command IDs, and subtransactions. The function ensures proper initialization of all fields, particularly setting the command_id to InvalidCommandId since zero is not the invalid value for command IDs.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer from which to allocate the new transaction

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [dlist_init](../d/dlist_init.md) (called 3 times for different lists)
  - InvalidCommandId
- Called from (representative examples):
  - IsInsertOrUpdate
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)

## Notes and Other Information
- This is a static (internal) function, not part of the public API
- Uses the txn_context slab allocator for efficient memory management
- Initializes three doubly-linked lists: changes, tuplecids, and subtxns
- Sets command_id to InvalidCommandId explicitly since it's not zero
- Zeroes out the entire structure before initialization to ensure clean state
- Sets output_plugin_private to NULL for plugin-specific data
- Memory is automatically freed when the reorder buffer's context is deleted

## Simplified Source

```c
static ReorderBufferTXN *
ReorderBufferGetTXN(ReorderBuffer *rb)
{
    ReorderBufferTXN *txn;

    // Allocate memory for new transaction from reorder buffer context
    txn = (ReorderBufferTXN *)
        MemoryContextAlloc(rb->txn_context, sizeof(ReorderBufferTXN));

    // Clear all fields to zero
    memset(txn, 0, sizeof(ReorderBufferTXN));

    // Initialize empty linked lists for transaction data
    dlist_init(&txn->changes);      // List of changes in this transaction
    dlist_init(&txn->tuplecids);    // List of tuple command IDs
    dlist_init(&txn->subtxns);      // List of subtransactions

    // Set command ID to invalid (not zero)
    txn->command_id = InvalidCommandId;
    txn->output_plugin_private = NULL;

    return txn;
}
```