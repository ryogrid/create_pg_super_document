# spgAddNodeAction

## Location
[src/backend/access/spgist/spgdoinsert.c:1513-1714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L1513-L1714)

## Overview
Adds a new node to an existing inner tuple, either by in-place replacement or by moving the enlarged tuple to a new page and updating parent references.

## Definition

```c
struct new inner tuple with additional node */
	newInnerTuple = addNode(state, innerTuple, nodeLabel, nodeN);
```
## Detailed Description
The  function handles the "addNode" operation requested by the opclass choose function. It creates a new version of the inner tuple with an additional node inserted at the specified position. If the enlarged tuple fits on the current page, it performs in-place replacement. Otherwise, it allocates a new page, moves the tuple there, updates the parent's downlink, and replaces the original tuple with either a redirection tuple (during normal operation) or a placeholder (during index build) to maintain tuple offset stability for existing downlinks.

## Parameters / Member Variables
- : The SP-GiST index relation being modified
- : SP-GiST state information containing opclass configuration
- : The existing inner tuple to be enlarged with a new node
- : Page descriptor for the page containing the inner tuple
- : Page descriptor for the parent page (needed if tuple must be moved)
- : Position where the new node should be inserted
- : Label value for the new node being added

## Dependencies
- Functions called/Symbols referenced:
  - [addNode](../a/addNode.md)
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [saveNodeLink](saveNodeLink.md)
  - [spgFormDeadTuple](spgFormDeadTuple.md)
- Called from (representative examples):
  - [spgdoinsert](spgdoinsert.md)

## Notes and Other Information
The function includes comprehensive WAL logging for crash recovery when not in build mode. It cannot be applied to nulls pages and will error if attempted on the root page when enlargement would exceed page capacity. The function carefully manages buffer relationships, ensuring that parent buffer updates are properly coordinated when the tuple is moved to a different page. During index build, placeholder tuples are used instead of redirection tuples for better performance since concurrent scans are not a concern.