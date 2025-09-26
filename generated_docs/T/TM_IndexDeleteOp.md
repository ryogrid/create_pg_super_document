# TM_IndexDeleteOp

## Location
[src/include/access/tableam.h:246-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L246-L257)

## Overview
TM_IndexDeleteOp is a comprehensive structure that orchestrates index tuple deletion operations, containing both operation parameters and mutable arrays of deletion candidates with their associated status information.

## Definition
```c
typedef struct TM_IndexDeleteOp
{
    Relation     irel;              /* Target index relation */
    BlockNumber  iblknum;           /* Index block number (for error reports) */
    bool         bottomup;          /* Bottom-up (not simple) deletion? */
    int          bottomupfreespace; /* Bottom-up space target */
    
    /* Mutable per-TID information follows (index AM initializes entries) */
    int              ndeltids;      /* Current # of deltids/status elements */
    TM_IndexDelete  *deltids;
    TM_IndexStatus  *status;
} TM_IndexDeleteOp;
```

## Detailed Description
This structure serves as the central coordination mechanism between index access methods and table access methods during index tuple deletion operations. It encapsulates both the high-level parameters of the deletion operation and the detailed per-tuple information needed to execute the deletion efficiently.

The structure supports two distinct deletion strategies: simple index deletion for known-dead tuples, and bottom-up index deletion which performs speculative work to identify additional deletion candidates. The bottom-up approach is particularly sophisticated, using hints and space targets to optimize the cost-benefit ratio of deletion operations.

The structure's design reflects PostgreSQL's philosophy of coordination between access method layers, where the index AM provides guidance and targets while the table AM maintains control over the actual deletion decisions based on transaction status and visibility information.

## Parameters / Member Variables
- `irel`: Relation pointer to the target index relation being operated on
- `iblknum`: Block number within the index for error reporting and diagnostic purposes
- `bottomup`: Boolean flag indicating whether this is a bottom-up deletion operation (true) or simple deletion operation (false)
- `bottomupfreespace`: Target amount of free space (in bytes) that the bottom-up deletion operation should aim to achieve; set to zero for simple deletion operations
- `ndeltids`: Current number of elements in both the deltids and status arrays, which can change during processing as the tableam evaluates candidates
- `deltids`: Pointer to dynamically allocated array of TM_IndexDelete structures containing TID information for deletion candidates
- `status`: Pointer to dynamically allocated array of TM_IndexStatus structures containing detailed status information corresponding to each deltids entry

## Dependencies
- Functions called/Symbols referenced:
  - [TM_IndexDelete](TM_IndexDelete.md) (component structure)
  - [TM_IndexStatus](TM_IndexStatus.md) (component structure)  
  - [Relation](../R/Relation.md) (PostgreSQL relation type)
  - BlockNumber (PostgreSQL block number type)
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)
  - [index_delete_check_htid](../i/index_delete_check_htid.md)
  - [_bt_bottomupdel_pass](../b/_bt_bottomupdel_pass.md)
  - [_bt_simpledel_pass](../b/_bt_simpledel_pass.md)
  - [table_index_delete_tuples](../t/table_index_delete_tuples.md)
  - [index_compute_xid_horizon_for_tuples](../i/index_compute_xid_horizon_for_tuples.md)

## Notes and Other Information
- Central to PostgreSQL's sophisticated index maintenance and space reclamation strategies
- The deltids and status arrays are conceptually one logical array but separated for performance reasons - keeping TM_IndexDelete small enables efficient sorting
- Bottom-up deletion uses "promising" hints from the index AM to guide speculative work by the table AM
- The structure enables efficient coordination between B-tree index operations and heap table operations
- Arrays may be sorted and resized during processing, with the index AM using idxoffnum to track correlations
- Critical component of PostgreSQL's MVCC-aware index cleanup mechanisms