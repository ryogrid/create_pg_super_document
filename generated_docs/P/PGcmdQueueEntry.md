# PGcmdQueueEntry

## Location
[src/interfaces/libpq/libpq-int.h:337-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-int.h#L337-L342)

## Overview
PGcmdQueueEntry represents a single entry in libpq's pending command queue, used to manage pipelined queries and commands in PostgreSQL connections.

## Definition
```c
typedef struct PGcmdQueueEntry
{
    PGQueryClass queryclass;            /* Query type */
    char       *query;                  /* SQL command, or NULL if none/unknown/OOM */
    struct PGcmdQueueEntry *next;       /* list link */
} PGcmdQueueEntry;
```

## Detailed Description
The PGcmdQueueEntry structure implements a singly-linked list node for managing queued commands in libpq's pipeline mode. When applications send multiple queries without waiting for results (pipeline mode), these commands are queued using this structure. Each entry stores the type of query operation, the SQL command text (if applicable), and a pointer to the next entry in the queue. The structure supports libpq's efficient memory management by allowing entries to be recycled rather than immediately freed, reducing malloc/free overhead in high-throughput scenarios. The queryclass field distinguishes between different types of PostgreSQL protocol operations, enabling appropriate handling for each command type.

## Parameters / Member Variables
- `queryclass`: Enum value of type PGQueryClass indicating the type of operation (PGQUERY_SIMPLE, PGQUERY_EXTENDED, PGQUERY_PREPARE, PGQUERY_DESCRIBE, PGQUERY_SYNC, PGQUERY_CLOSE)
- `query`: Pointer to null-terminated SQL command string. May be NULL for commands that don't have associated SQL text or in out-of-memory conditions
- `next`: Pointer to the next PGcmdQueueEntry in the linked list, NULL for the last entry in the queue

## Dependencies
- Functions called/Symbols referenced:
  - PGQueryClass (enum for query classification)
  - Self-references via next pointer for linked list structure
- Used by:
  - [Command](../C/Command.md) queue management functions (pqFreeCommandQueue, pqAllocCmdQueueEntry, pqAppendCmdQueueEntry, pqRecycleCmdQueueEntry)
  - [Query](../Q/Query.md) execution functions (PQsendQueryInternal, PQsendPrepare, PQsendQueryGuts, PQsendTypedCommand)
  - Pipeline management (pqCommandQueueAdvance, pqPipelineSyncInternal)
  - Connection structure fields (cmd_queue_head, cmd_queue_tail, cmd_queue_recycle in libpq-int.h:464-471)

## Notes and Other Information
- Used in libpq's pipeline mode to queue multiple commands before sending them to the server
- Supports memory optimization through a recycling mechanism (cmd_queue_recycle) to avoid frequent malloc/free operations
- The query field may be NULL in cases where the command doesn't have associated SQL text or during out-of-memory conditions
- Part of the command queue infrastructure maintained as head/tail pointers in the pg_conn structure
- Essential for supporting PostgreSQL's extended query protocol and pipeline mode functionality
- The queryclass field enables libpq to handle different protocol message types appropriately (Simple Query, Extended Query, Prepare, Describe, Sync, Close)