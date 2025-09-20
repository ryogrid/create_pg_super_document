# dlist_push_tail

## Location
[src/include/lib/ilist.h:364-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L364-L380)

## Overview
Inserts a new node at the end of a doubly-linked list, automatically handling both initialized and uninitialized list states.

## Definition

```c
static inline void
dlist_push_tail(dlist_head *head, dlist_node *node)
```
## Detailed Description
The  function adds a new node to the back of a doubly-linked list by updating the necessary pointer relationships. Like , it intelligently handles uninitialized lists by checking if the head's next pointer is NULL and automatically calling  to convert it to a proper circular structure. The function then inserts the new node between the current last element and the head, updating all four relevant pointers: the new node's next pointer (to head), the new node's prev pointer (to the old tail), the old tail's next pointer (to the new node), and the head's prev pointer (to the new node). After insertion, it calls  to validate list integrity in debug builds.

## Parameters / Member Variables
- : Pointer to the  structure representing the list to insert into
- : Pointer to the  structure to be inserted at the end of the list

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (structure type)
  - [dlist_node](dlist_node.md) (structure type)
  - [dlist_init](dlist_init.md) (initialization function)
  - [dlist_check](dlist_check.md) (integrity validation function)
- Called from (representative examples):
  - [disassembleLeaf](disassembleLeaf.md) (src/backend/access/gin/gindatapage.c:1396)
  - [addItemsToLeaf](../a/addItemsToLeaf.md) (src/backend/access/gin/gindatapage.c:1463)
  - [cache_lookup](../c/cache_lookup.md) (src/backend/executor/nodeMemoize.c:571)
  - [ReorderBufferTXNByXid](../R/ReorderBufferTXNByXid.md) (src/backend/replication/logical/reorderbuffer.c:710)
  - [ReorderBufferQueueChange](../R/ReorderBufferQueueChange.md) (src/backend/replication/logical/reorderbuffer.c:849)
  - [SetupLockInTable](../S/SetupLockInTable.md) (src/backend/storage/lmgr/lock.c:1286)
  - [CreatePredXact](../C/CreatePredXact.md) (src/backend/storage/lmgr/predicate.c:591)
  - InitProcGlobal (src/backend/storage/lmgr/proc.c:244)

## Notes and Other Information
- The function is implemented as a static inline function for performance efficiency
- Automatically initializes uninitialized lists (NULL head) by calling 
- Maintains proper doubly-linked list invariants by updating all necessary pointer relationships
- Includes integrity checking via  in debug builds
- Extensively used in PostgreSQL's GIN indexing, logical replication, locking, predicate locking, and memory management subsystems
- The node being inserted should not already be part of another list to avoid corruption
- Complements  for queue-like FIFO operations when used together
- Located in src/include/lib/ilist.h:364-380