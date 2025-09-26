# TM_IndexDelete

## Location
src/include/access/tableam.h: 212 - 216

## Overview
TM_IndexDelete is a structure that represents individual table tuples to be deleted during index tuple deletion operations, containing a table TID and an offset into a corresponding status array.

## Definition
```c
typedef struct TM_IndexDelete
{
    ItemPointerData tid;        /* table TID from index tuple */
    int16          id;          /* Offset into TM_IndexStatus array */
} TM_IndexDelete;
```

## Detailed Description
This structure is part of PostgreSQL's index deletion mechanism and works in conjunction with the table access method infrastructure. Each TM_IndexDelete entry represents a single table tuple that is being considered for deletion based on information gathered from index tuples. The structure is designed to be lightweight to enable efficient sorting operations during the deletion process.

The structure serves as a bridge between index access methods and table access methods, carrying the essential information needed to locate and process table tuples during index deletion operations. It's used in arrays that can be sorted and processed efficiently by the table access method implementation.

## Parameters / Member Variables
- `tid`: ItemPointerData containing the table TID (Tuple Identifier) obtained from the corresponding index tuple, identifying the specific table tuple to be considered for deletion
- `id`: A 16-bit integer offset into the associated TM_IndexStatus array, providing access to additional status information about this particular deletion candidate

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerData (embedded structure type)
- Called from (representative examples):
  - heap_index_delete_tuples
  - index_delete_sort
  - bottomup_sort_and_shrink
  - _bt_bottomupdel_pass
  - _bt_simpledel_pass
  - index_compute_xid_horizon_for_tuples

## Notes and Other Information
- This structure is typically used in arrays that are sorted by TID for efficient processing by block-oriented table access methods
- The separation of TM_IndexDelete and TM_IndexStatus into two arrays keeps the TM_IndexDelete structure small, which improves sorting performance
- Used extensively in both simple index deletion and bottom-up index deletion strategies
- The id field enables correlation between entries in the deltids array and their corresponding status information
- Critical component of PostgreSQL's index maintenance and space reclamation operations