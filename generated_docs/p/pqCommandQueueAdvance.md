# pqCommandQueueAdvance

## Location
[src/interfaces/libpq/fe-exec.c:3142-3179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3142-L3179)

## Overview
Removes a completed query from the head of the command queue when all corresponding results have been received, with protocol-specific synchronization handling.

## Definition

```c
void
pqCommandQueueAdvance(PGconn *conn, bool isReadyForQuery, bool gotSync)
```
## Detailed Description
pqCommandQueueAdvance manages the advancement of PostgreSQL's internal command queue by removing the head element when it's safe to do so. The function implements protocol-specific logic to ensure proper synchronization between sent commands and received results.

For simple query protocol, advancement only occurs when a ReadyForQuery message is received, since simple queries can contain multiple statements that must all complete. For extended query protocol, the function respects SYNC boundaries to maintain proper error recovery semantics, preventing advancement past a SYNC element unless the corresponding SYNC response is received.

When advancing, the function unlinks the head element, updates queue pointers, and recycles the queue entry for reuse.

## Parameters / Member Variables
- : The PostgreSQL connection containing the command queue
- : True if a ReadyForQuery message was received
- : True if a SYNC response was received

## Dependencies
- Functions called/Symbols referenced:
  - [pqRecycleCmdQueueEntry](pqRecycleCmdQueueEntry.md)
  - [PGcmdQueueEntry](../P/PGcmdQueueEntry.md)
  - PGQUERY_SIMPLE
  - PGQUERY_SYNC

- Called from (representative examples):
  - [PQgetResult](../P/PQgetResult.md) (fe-exec.c)
  - [pqParseInput3](pqParseInput3.md) (fe-protocol3.c)

## Notes and Other Information
- Only advances for simple queries when ReadyForQuery is received
- Blocks advancement past SYNC elements until corresponding SYNC response arrives
- Automatically resets queue tail pointer when queue becomes empty
- Recycles removed queue entries to avoid memory allocation overhead
- Critical for maintaining command/response synchronization in pipeline mode

## Simplified Source

```c
void
pqCommandQueueAdvance(PGconn *conn, bool isReadyForQuery, bool gotSync)
{
    PGcmdQueueEntry *prevquery;

    // Nothing to advance if queue is empty
    if (conn->cmd_queue_head == NULL)
        return;

    // For simple queries, wait for ReadyForQuery message
    if (conn->cmd_queue_head->queryclass == PGQUERY_SIMPLE && !isReadyForQuery)
        return;

    // For SYNC operations, wait for corresponding SYNC response
    if (conn->cmd_queue_head->queryclass == PGQUERY_SYNC && !gotSync)
        return;

    // Remove head element from queue
    prevquery = conn->cmd_queue_head;
    conn->cmd_queue_head = conn->cmd_queue_head->next;

    // Reset tail if queue becomes empty
    if (conn->cmd_queue_head == NULL)
        conn->cmd_queue_tail = NULL;

    // Recycle the removed queue entry
    prevquery->next = NULL;
    pqRecycleCmdQueueEntry(conn, prevquery);
}
```