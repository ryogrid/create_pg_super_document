# spgVacPendingItem

## Location
[src/backend/access/spgist/spgvacuum.c:32-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L32-L37)

## Overview
A structure representing an entry in a pending list of TIDs (Tuple IDs) that need to be revisited during SPGiST vacuum operations.

## Definition

```c
typedef struct spgVacPendingItem
{
	ItemPointerData tid;		/* redirection target to visit */
	bool		done;			/* have we dealt with this? */
	struct spgVacPendingItem *next; /* list link */
} spgVacPendingItem;
```
## Detailed Description
The  structure is used in SPGiST (Space-Partitioned Generalized Search Tree) vacuum operations to maintain a linked list of tuple identifiers that require processing. This structure is essential for tracking redirection targets that need to be visited during the vacuum process. The pending list ensures that all necessary tuples are processed even when they are discovered during the vacuum operation itself.

The structure implements a simple linked list where new items are always appended at the end to ensure that scans of the list don't miss items added during the scan process.

## Parameters / Member Variables
- : ItemPointerData containing the tuple identifier of the redirection target that needs to be visited
- : Boolean flag indicating whether this particular TID has been processed/dealt with during the vacuum operation
- : Pointer to the next spgVacPendingItem in the linked list, forming a singly-linked list structure

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md) (for storing tuple identifiers)
  - struct spgVacPendingItem (self-reference for linked list)
- Called from (representative examples):
  - [spgBulkDeleteState](spgBulkDeleteState.md) (contains pendingList field)
  - [spgAddPendingTID](spgAddPendingTID.md) (creates and manages pending items)
  - [spgClearPendingList](spgClearPendingList.md) (iterates and frees pending items)
  - [spgprocesspending](spgprocesspending.md) (processes pending items)

## Notes and Other Information
- New items are always appended at the end of the list to maintain scan consistency
- The  flag is used to track processing status and is asserted to be true when clearing the list
- Memory for pending items is allocated using  and freed using 
- This structure is specifically designed for SPGiST vacuum operations and is not used in other contexts
- The linked list design allows for dynamic addition of items during vacuum processing without affecting ongoing scans