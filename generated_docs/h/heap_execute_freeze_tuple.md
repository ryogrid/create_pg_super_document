# heap_execute_freeze_tuple

## Location
[src/backend/access/heap/heapam.c:7283-7305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7283-L7305)

## Overview
A static inline function that executes a prepared freeze plan by applying the specified changes to a tuple's transaction ID fields and infomask bits, completing the tuple freezing process.

## Definition

```c
static inline void
heap_execute_freeze_tuple(HeapTupleHeader tuple, HeapTupleFreeze *frz)
```
## Detailed Description
heap_execute_freeze_tuple is the execution phase of PostgreSQL's tuple freezing mechanism. This function takes a freeze plan prepared by heap_prepare_freeze_tuple and applies all the specified modifications to the actual tuple header. The function is designed to be simple and fast, performing only the mechanical updates specified in the freeze plan.

The function performs these operations based on the freeze plan:
1. Sets the new xmax value as determined during the preparation phase
2. Handles xvac field modifications for old-style VACUUM FULL scenarios
3. Applies updated infomask and infomask2 bits to reflect the new transaction state
4. Ensures atomic application of all changes specified in the freeze plan

This function assumes that all validation and safety checks were performed during the preparation phase, focusing purely on executing the predetermined changes efficiently.

## Parameters
- : Pointer to the tuple header to be modified with the freeze plan
- : Freeze plan structure containing all the changes to apply to the tuple

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderSetXmax
  - HeapTupleHeaderSetXvac
  - FrozenTransactionId
  - InvalidTransactionId
  - XLH_FREEZE_XVAC
  - XLH_INVALID_XVAC
- Called from:
  - [heap_freeze_prepared_tuples](heap_freeze_prepared_tuples.md)
  - [heap_freeze_tuple](heap_freeze_tuple.md)
  - [heap_xlog_prune_freeze](heap_xlog_prune_freeze.md)

## Notes and Other Information
- **Concurrency Safety**: Caller must ensure exclusive access to the tuple storage (via buffer lock or private storage)
- **Atomic Updates**: All changes are applied as a unit, ensuring tuple consistency
- **No Validation**: Function assumes all validation was done during freeze plan preparation
- **Performance Optimized**: Inline function designed for minimal overhead during execution
- **Xvac Handling**: Supports both freezing and invalidating xvac for old-style VACUUM FULL compatibility
- **Infomask Updates**: Applies both t_infomask and t_infomask2 changes to reflect new transaction states
- **WAL Integration**: Used in WAL recovery scenarios to replay freeze operations
- **Buffer Requirements**: Typically used with shared buffers that have been exclusively locked

## Simplified Source

```c
static inline void heap_execute_freeze_tuple(HeapTupleHeader tuple, HeapTupleFreeze *frz)
{
    // Apply the new xmax value from freeze plan
    HeapTupleHeaderSetXmax(tuple, frz->xmax);

    // Handle xvac field modifications based on freeze flags
    if (frz->frzflags & XLH_FREEZE_XVAC)
        HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);

    if (frz->frzflags & XLH_INVALID_XVAC)
        HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);

    // Apply updated infomask bits to reflect new transaction state
    tuple->t_infomask = frz->t_infomask;
    tuple->t_infomask2 = frz->t_infomask2;
}
```