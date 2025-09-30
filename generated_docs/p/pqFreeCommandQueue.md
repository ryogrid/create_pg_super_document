# pqFreeCommandQueue

## Location
[src/interfaces/libpq/fe-connect.c:558-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L558-L583)

## Overview
Frees all entries in a PGcmdQueueEntry linked list, deallocating memory for queued PostgreSQL commands.

## Definition
```c
static void pqFreeCommandQueue(PGcmdQueueEntry *queue)
```

## Detailed Description
This static function traverses a linked list of PGcmdQueueEntry structures and properly deallocates each entry. It is responsible for freeing both the query string stored in each entry and the entry structure itself. The function iterates through the entire queue, ensuring no memory leaks occur when cleaning up pending or processed commands.

This function is essential for connection cleanup and memory management in libpq's command pipelining functionality.

## Parameters / Member Variables
- `queue`: Pointer to the head of a linked list of PGcmdQueueEntry structures to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [PGcmdQueueEntry](../P/PGcmdQueueEntry.md) (struct type)
  - free (standard library function)

- Called from (representative examples):
  - [pqDropConnection](pqDropConnection.md) (twice - for cmd_queue_head and cmd_queue_recycle)
  - internalPQconninfoOption

## Notes and Other Information
- The function safely handles NULL input by checking the queue pointer in the while loop condition
- Properly manages the linked list traversal to avoid accessing freed memory
- Part of libpq's command pipelining infrastructure for managing multiple commands
- Critical for preventing memory leaks when connections are dropped or reset
- The function is static, indicating it's only used within the fe-connect.c file

## Simplified Source

```c
static void
pqFreeCommandQueue(PGcmdQueueEntry *queue)
{
    while (queue != NULL) {
        PGcmdQueueEntry *cur = queue;

        // Move to next entry before freeing current
        queue = cur->next;
        free(cur->query);
        free(cur);
    }
}
```