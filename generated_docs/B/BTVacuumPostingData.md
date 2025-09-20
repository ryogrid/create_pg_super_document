# BTVacuumPostingData

## Location
[src/include/access/nbtree.h:903-912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L903-L912)

## Overview
BTVacuumPostingData is a state structure used during B-tree VACUUM operations to represent how to process a posting list tuple when some (but not all) of its TIDs are to be deleted.

## Definition

```c
typedef struct BTVacuumPostingData
{
	/* Tuple that will be/was updated */
	IndexTuple	itup;
	OffsetNumber updatedoffset;

	/* State needed to describe final itup in WAL */
	uint16		ndeletedtids;
	uint16		deletetids[FLEXIBLE_ARRAY_MEMBER];
} BTVacuumPostingData;
```
## Detailed Description
This structure manages the vacuum process for posting list tuples in B-tree indexes. When VACUUM determines that only some TIDs in a posting list tuple need to be deleted (rather than the entire tuple), this structure maintains the state needed for the operation. The convention is that the itup field contains the original posting list tuple on input and a palloc()'d final tuple used to overwrite the existing tuple on output.

## Parameters / Member Variables
- `itup`: IndexTuple that will be or was updated during the vacuum operation
- `updatedoffset`: OffsetNumber indicating the offset of the tuple being updated
- `ndeletedtids`: Number of TIDs that were deleted from the posting list
- `deletetids[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing the TIDs that are to be deleted
## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - [IndexTuple](../I/IndexTuple.md)
  - OffsetNumber
- Called from (representative examples):
  - [_bt_delitems_delete_check](../b/_bt_delitems_delete_check.md)
  - [btreevacuumposting](../b/btreevacuumposting.md)
  - [btree_xlog_updates](../b/btree_xlog_updates.md)
  - BTVacuumPosting

## Notes and Other Information
- This structure is essential for partial posting list cleanup during VACUUM operations
- The flexible array member deletetids allows for variable-length storage of TID information
- Used in WAL logging to describe the final state of the updated tuple
- Part of the B-tree access method implementation for efficient index maintenance