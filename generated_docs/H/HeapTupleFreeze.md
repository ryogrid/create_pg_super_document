# HeapTupleFreeze

## Location
src/include/access/heapam.h: 140 - 152

## Overview
HeapTupleFreeze is a structure that describes how to freeze a specific heap tuple, containing the necessary state and flags for transaction ID freezing operations during VACUUM.

## Definition
```c
typedef struct HeapTupleFreeze
{
    /* Fields describing how to process tuple */
    TransactionId xmax;         /* new xmax value (or unchanged) */
    uint16        t_infomask2;  /* new t_infomask2 value */
    uint16        t_infomask;   /* new t_infomask value */
    uint8         frzflags;     /* freezing operation flags */
    
    /* xmin/xmax check flags */
    uint8         checkflags;   /* validation flags for freeze operation */
    /* Page offset number for tuple */
    OffsetNumber  offset;       /* tuple's offset within the page */
} HeapTupleFreeze;
```

## Detailed Description
HeapTupleFreeze serves as a freeze plan descriptor that specifies exactly how a heap tuple should be processed during transaction ID freezing operations. This structure is prepared by heap_prepare_freeze_tuple() and later executed by heap_execute_freeze_tuple() to safely freeze old transaction IDs that are older than the freeze cutoff thresholds. The structure contains the new values for the tuple's transaction-related fields (xmax, t_infomask, t_infomask2) and control flags that guide the freezing process, including validation requirements to ensure data integrity during the freeze operation.

## Parameters / Member Variables
- `xmax`: New xmax transaction ID value to be written to the tuple (may be unchanged, InvalidTransactionId, or a replacement MultiXactId)
- `t_infomask2`: New value for the tuple's t_infomask2 field containing updated hint bits and flags
- `t_infomask`: New value for the tuple's t_infomask field with updated transaction status and type flags
- `frzflags`: Freezing operation control flags (e.g., XLH_FREEZE_XVAC, XLH_INVALID_XVAC) that specify the type of freeze operation
- `checkflags`: Validation flags (e.g., HEAP_FREEZE_CHECK_XMIN_COMMITTED, HEAP_FREEZE_CHECK_XMAX_ABORTED) for transaction status verification
- `offset`: Page offset number identifying which tuple on the page this freeze plan applies to

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId
  - OffsetNumber
- Called from (representative examples):
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)
  - [heap_execute_freeze_tuple](../h/heap_execute_freeze_tuple.md)
  - [heap_pre_freeze_checks](../h/heap_pre_freeze_checks.md)
  - [heap_freeze_prepared_tuples](../h/heap_freeze_prepared_tuples.md)
  - [heap_freeze_tuple](../h/heap_freeze_tuple.md)
  - [heap_xlog_prune_freeze](../h/heap_xlog_prune_freeze.md)
  - [heap_log_freeze_plan](../h/heap_log_freeze_plan.md)
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md)

## Notes and Other Information
- Used as output from heap_prepare_freeze_tuple() to describe the freeze plan for a specific tuple
- The structure ensures atomicity of complex freeze operations involving MultiXactId processing
- checkflags enable verification that transaction states match expectations before applying changes
- frzflags control WAL logging behavior and specify the type of freeze operation being performed
- Essential for VACUUM's tuple freezing process to prevent transaction ID wraparound
- The offset field allows batch processing of multiple freeze plans for tuples on the same page
- Freeze plans may involve complex MultiXactId transformations or simple transaction ID clearing
- Critical for maintaining data consistency during transaction ID space management operations