# dlist_next_node

## Location
[src/include/lib/ilist.h:537-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L537-L546)

## Overview
Returns the next node in a doubly-linked list, with an assertion to ensure that a next node actually exists.

## Definition

```c
static inline dlist_node *
dlist_next_node(dlist_head *head, dlist_node *node)
```
## Detailed Description
This function provides safe navigation to the next node in a doubly-linked list by returning the node's next pointer. Before returning the pointer, it uses an assertion to verify that a next node actually exists by calling dlist_has_next(). This prevents accidental traversal beyond the end of the list, which could lead to accessing invalid memory or the sentinel node.

The function is implemented as a static inline function for performance reasons, as list traversal operations are common and benefit from the elimination of function call overhead. The assertion serves as both documentation (indicating the precondition) and a runtime safety check in debug builds.

## Parameters / Member Variables
- : Pointer to the list head structure, used by the assertion to verify that a next node exists
- : Pointer to the current node whose next node should be returned

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](dlist_head.md) (struct type)
  - [dlist_node](dlist_node.md) (struct type)
  - [dlist_has_next](dlist_has_next.md) (function to verify next node exists)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - [dataPlaceToPageLeafSplit](dataPlaceToPageLeafSplit.md) (src/backend/access/gin/gindatapage.c:1058, 1061, 1082)
  - [addItemsToLeaf](../a/addItemsToLeaf.md) (src/backend/access/gin/gindatapage.c:1485)
  - [leafRepackItems](../l/leafRepackItems.md) (src/backend/access/gin/gindatapage.c:1597, 1716)
  - [ReorderBufferIterTXNNext](../R/ReorderBufferIterTXNNext.md) (src/backend/replication/logical/reorderbuffer.c:1440)
  - [pgstat_flush_pending_entries](../p/pgstat_flush_pending_entries.md) (src/backend/utils/activity/pgstat.c:1219)
  - [dclist_next_node](dclist_next_node.md) (src/include/lib/ilist.h:871)

## Notes and Other Information
- The function assumes that the caller has verified a next node exists, enforced by the assertion
- Used extensively in PostgreSQL's GIN index operations and replication logic
- Part of the safe list traversal API that prevents common list iteration errors
- The assertion helps catch programming errors where code attempts to traverse beyond list boundaries