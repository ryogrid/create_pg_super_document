# dlist_node_init

## Location
[src/include/lib/ilist.h:325-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L325-L335)

## Overview
Initializes a doubly-linked list node to a detached state by setting its pointers to NULL, enabling safe detection of unlinked nodes.

## Definition

```c
static inline void
dlist_node_init(dlist_node *node)
```
## Detailed Description
The  function initializes a doubly-linked list node by setting both its  and  pointers to NULL. This creates a detached node state that can be safely detected using . The function is specifically designed for scenarios where it's necessary to determine whether a node is currently linked to a list or exists in an unattached state. This initialization is particularly useful in resource management and transaction processing where nodes may be temporarily detached and later reattached to lists.

## Parameters / Member Variables
- : Pointer to the  structure that will be initialized to a detached state

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](dlist_node.md) (structure type)
- Called from (representative examples):
  - [MarkAsPreparingGuts](../M/MarkAsPreparingGuts.md) (src/backend/access/transam/twophase.c:446)
  - [InitPredicateLocks](../I/InitPredicateLocks.md) (src/backend/storage/lmgr/predicate.c:1264)
  - [GetSerializableTransactionSnapshotInt](../G/GetSerializableTransactionSnapshotInt.md) (src/backend/storage/lmgr/predicate.c:1855)
  - InitProcess (src/backend/storage/lmgr/proc.c:384)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (src/backend/storage/lmgr/proc.c:582)

## Notes and Other Information
- This function is implemented as a static inline function for performance efficiency
- The NULL pointer initialization enables the use of  to check node status
- Primarily used in PostgreSQL's transaction management and predicate locking systems
- Essential for safe node lifecycle management where nodes may exist independently of lists
- Located in src/include/lib/ilist.h:325-335