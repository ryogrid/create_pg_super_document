# pqRecycleCmdQueueEntry

## Location
[src/interfaces/libpq/fe-exec.c:1386-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1386-L1415)

## Overview
Recycles a command queue entry by cleaning up its resources and adding it to the connection's recycle queue for future reuse, implementing a memory pooling strategy.

## Definition
```c
static void pqRecycleCmdQueueEntry(PGconn *conn, PGcmdQueueEntry *entry)
```

## Detailed Description
This function implements the cleanup and recycling mechanism for PostgreSQL command queue entries. It safely handles NULL entries by returning early, then performs resource cleanup by freeing any allocated query string memory. After cleanup, the entry is added to the head of the connection's recycle queue (cmd_queue_recycle) for future reuse by pqAllocCmdQueueEntry.

The function ensures proper memory management by freeing the query string if present and resetting the query pointer to NULL before recycling. It maintains the recycle queue as a simple linked list where new entries are added to the head for efficient LIFO (Last In, First Out) access.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object that maintains the recycle queue
- `entry`: Pointer to the command queue entry to be recycled (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - free (standard library function)
  - Assert (debugging macro)
  - [PGcmdQueueEntry](../P/PGcmdQueueEntry.md) (struct type)
- Called from (representative examples):
  - [PQsendQueryInternal](../P/PQsendQueryInternal.md)
  - [PQsendPrepare](../P/PQsendPrepare.md)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md)
  - [PQsendTypedCommand](../P/PQsendTypedCommand.md)
  - [pqCommandQueueAdvance](pqCommandQueueAdvance.md)
  - [pqPipelineSyncInternal](pqPipelineSyncInternal.md)

## Notes and Other Information
- This is a static function, only accessible within fe-exec.c
- Safely handles NULL entry pointers by returning early
- Asserts that recyclable entries should not have a follow-on command (entry->next should be NULL)
- Frees memory allocated for the query string to prevent memory leaks
- Implements LIFO recycling strategy for optimal cache locality
- Essential part of PostgreSQL's memory management optimization for frequent command queue operations
- Used both for error cleanup and normal command processing completion

## Simplified Source
```c
static void pqRecycleCmdQueueEntry(PGconn *conn, PGcmdQueueEntry *entry) {
    // Handle NULL entry safely
    if (entry == NULL)
        return;

    // Entry should not have follow-on commands when recycled
    Assert(entry->next == NULL);

    // Free allocated query string if present
    if (entry->query) {
        free(entry->query);
        entry->query = NULL;
    }

    // Add entry to head of recycle queue
    entry->next = conn->cmd_queue_recycle;
    conn->cmd_queue_recycle = entry;
}
```