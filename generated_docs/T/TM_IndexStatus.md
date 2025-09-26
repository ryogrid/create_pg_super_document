# TM_IndexStatus

## Location
src/include/access/tableam.h: 218 - 226

## Overview
TM_IndexStatus is a structure that holds status information for individual index tuples during index deletion operations, tracking their deletability status and providing optimization hints for bottom-up deletion strategies.

## Definition
```c
typedef struct TM_IndexStatus
{
    OffsetNumber idxoffnum;      /* Index am page offset number */
    bool         knowndeletable; /* Currently known to be deletable? */
    
    /* Bottom-up index deletion specific fields follow */
    bool         promising;      /* Promising (duplicate) index tuple? */
    int16        freespace;      /* Space freed in index if deleted */
} TM_IndexStatus;
```

## Detailed Description
This structure serves as a companion to TM_IndexDelete, providing detailed status information about each index tuple being considered for deletion. It supports both simple index deletion and the more complex bottom-up index deletion strategy. The structure enables the table access method to communicate back to the index access method about the feasibility and benefits of deleting specific index tuples.

The separation of this status information from the basic TID information in TM_IndexDelete allows for more efficient sorting and processing of the deletion candidates while maintaining detailed state about each entry.

## Parameters / Member Variables
- `idxoffnum`: The page offset number within the index access method's page, allowing correlation between the status information and the actual index tuple location
- `knowndeletable`: Boolean flag indicating whether the tuple is currently known to be safely deletable by the table access method
- `promising`: Boolean flag used specifically for bottom-up index deletion to mark index tuples that are promising candidates (typically duplicates) for deletion
- `freespace`: A 16-bit integer indicating the amount of space (in bytes) that would be freed in the index if this particular tuple were deleted, used for cost-benefit analysis in bottom-up deletion

## Dependencies  
- Functions called/Symbols referenced:
  - OffsetNumber (type definition)
- Called from (representative examples):
  - heap_index_delete_tuples
  - index_delete_check_htid
  - bottomup_sort_and_shrink
  - _bt_bottomupdel_pass
  - _bt_simpledel_pass
  - index_compute_xid_horizon_for_tuples

## Notes and Other Information
- The promising and freespace fields are specifically designed for bottom-up index deletion optimization, which performs speculative work to identify the most beneficial tuples to delete
- This structure enables sophisticated coordination between index and table access methods during deletion operations
- The knowndeletable field can change during processing as the table access method determines the actual deletability of tuples
- Used extensively in B-tree index maintenance operations for space reclamation
- Critical for implementing efficient index cleanup strategies that minimize I/O while maximizing space recovery