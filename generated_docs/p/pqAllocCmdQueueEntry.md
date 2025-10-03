# pqAllocCmdQueueEntry

## Location
[src/interfaces/libpq/fe-exec.c:1306-1338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1306-L1338)

## Overview
Allocates a command queue entry for the caller to fill, either by recycling an existing entry from the recycle queue or allocating a new one if no recycled entries are available.

## Definition

```c
static PGcmdQueueEntry *
pqAllocCmdQueueEntry(PGconn *conn)
```
## Detailed Description
This function manages memory allocation for PostgreSQL command queue entries with an optimization for memory reuse. It first checks if there are any entries in the connection's recycle queue (). If a recycled entry is available, it removes and returns that entry. If no recycled entries exist, it allocates a new  structure using . The function initializes the returned entry by setting both  and  pointers to NULL, ensuring a clean state for the caller.

The function implements a simple memory pooling strategy to reduce the overhead of frequent malloc/free operations when handling multiple PostgreSQL commands.

## Parameters / Member Variables
- `*conn`: A pointer to the PostgreSQL connection object that contains the command queue and recycle queue
## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [PGcmdQueueEntry](../P/PGcmdQueueEntry.md) (struct type)
- Called from (representative examples):
  - [PQsendQueryInternal](../P/PQsendQueryInternal.md)
  - [PQsendPrepare](../P/PQsendPrepare.md)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md)
  - [PQsendTypedCommand](../P/PQsendTypedCommand.md)
  - [pqPipelineSyncInternal](pqPipelineSyncInternal.md)

## Notes and Other Information
- This is a static function, only accessible within fe-exec.c
- The caller is responsible for either adding the returned entry to the command queue using  or recycling it using  if an error occurs
- Returns NULL and sets an error message if memory allocation fails
- The function follows PostgreSQL's memory management patterns for efficient command queuing in pipeline mode
- Part of the libpq command queuing system that supports asynchronous and pipelined query execution

## Simplified Source

```c
static PGcmdQueueEntry *pqAllocCmdQueueEntry(PGconn *conn) {
    PGcmdQueueEntry *entry;

    // Try to reuse an entry from recycle queue first
    if (conn->cmd_queue_recycle == NULL) {
        // No recycled entries available, allocate new one
        entry = malloc(sizeof(PGcmdQueueEntry));
        if (entry == NULL) {
            libpq_append_conn_error(conn, "out of memory");
            return NULL;
        }
    } else {
        // Reuse recycled entry
        entry = conn->cmd_queue_recycle;
        conn->cmd_queue_recycle = entry->next;
    }

    // Initialize entry to clean state
    entry->next = NULL;
    entry->query = NULL;

    return entry;
}
```