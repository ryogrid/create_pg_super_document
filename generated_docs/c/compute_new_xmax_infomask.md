# compute_new_xmax_infomask

## Location
[src/backend/access/heap/heapam.c:5280-5560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L5280-L5560)

## Overview
compute_new_xmax_infomask computes new Xmax and corresponding infomask values when a transaction acquires a new lock on a tuple, handling complex scenarios involving existing locks, MultiXactIds, and transaction states.

## Definition
```c
static void compute_new_xmax_infomask(TransactionId xmax, uint16 old_infomask,
                                      uint16 old_infomask2, TransactionId add_to_xmax,
                                      LockTupleMode mode, bool is_update,
                                      TransactionId *result_xmax, uint16 *result_infomask,
                                      uint16 *result_infomask2)
```

## Detailed Description
This critical static function orchestrates the complex logic for determining new Xmax and infomask values when acquiring tuple locks. It handles multiple scenarios including invalid previous locks, existing MultiXactIds, committed updates, and in-progress transactions.

The function implements sophisticated optimization logic, such as detecting when existing MultiXactIds are no longer running and can be simplified to single transaction IDs. It also handles race conditions where transactions may finish between the initial visibility check and this computation.

Key behaviors include:
- Creating new MultiXactIds when multiple lockers/updaters are present
- Optimizing cases where all previous lockers have finished
- Handling pg_upgrade compatibility with legacy lock formats
- Managing lock strength escalation for the same transaction
- Preserving committed updaters while adding new lockers

The function uses a goto-based state machine (label l5) to restart computation when conditions change, ensuring atomic and consistent lock state transitions.

## Parameters / Member Variables
- `xmax`: Current Xmax value from the tuple header
- `old_infomask`: Current infomask value from tuple header
- `old_infomask2`: Current infomask2 value from tuple header
- `add_to_xmax`: Transaction ID acquiring the new lock (must be current transaction)
- `mode`: LockTupleMode specifying the type of lock being acquired
- `is_update`: Boolean indicating whether this is for an update operation
- `result_xmax`: Output parameter for computed new Xmax value
- `result_infomask`: Output parameter for computed new infomask value
- `result_infomask2`: Output parameter for computed new infomask2 value

## Dependencies
- Functions called/Symbols referenced:
  - [get_mxact_status_for_lock](../g/get_mxact_status_for_lock.md)
  - [MultiXactIdExpand](../M/MultiXactIdExpand.md)
  - [MultiXactIdCreate](../M/MultiXactIdCreate.md)
  - [GetMultiXactIdHintBits](../G/GetMultiXactIdHintBits.md)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md)
  - [MultiXactIdGetUpdateXid](../M/MultiXactIdGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - TUPLOCK_from_mxstatus
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [heap_lock_tuple](../h/heap_lock_tuple.md)
  - [heap_lock_updated_tuple_rec](../h/heap_lock_updated_tuple_rec.md)

## Notes and Other Information
- This is a static function internal to heapam.c handling core tuple locking logic
- Contains extensive race condition handling for concurrent transaction scenarios
- The function may create new MultiXactIds as a side effect, impacting system resources
- Uses assertion to ensure add_to_xmax is always the current transaction ID
- Implements complex optimization paths to avoid unnecessary MultiXactId creation
- The goto l5 pattern allows restarting computation when lock states are simplified
- Critical for maintaining ACID properties and proper concurrent access control
- Handles backward compatibility with tuples upgraded by pg_upgrade