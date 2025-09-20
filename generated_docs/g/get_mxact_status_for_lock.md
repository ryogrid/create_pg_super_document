# get_mxact_status_for_lock

## Location
[src/backend/access/heap/heapam.c:4485-4532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L4485-L4532)

## Overview
get_mxact_status_for_lock is a static helper function that maps tuple lock modes to their corresponding MultiXactStatus values, distinguishing between update and non-update lock operations.

## Definition

```c
static MultiXactStatus
get_mxact_status_for_lock(LockTupleMode mode, bool is_update)
```
## Detailed Description
This function serves as a translation layer between PostgreSQL's tuple locking modes and MultiXact status values. It consults the tupleLockExtraInfo array to determine the appropriate MultiXactStatus based on the lock mode and whether the operation is an update. The function validates the input parameters and throws an error if an invalid combination is provided.

The function operates by:
1. Checking the is_update flag to determine which status field to use
2. Looking up the corresponding status in tupleLockExtraInfo array
3. Validating that the returned status is valid (not -1)
4. Returning the appropriate MultiXactStatus value

## Parameters / Member Variables
- : LockTupleMode indicating the type of lock being requested
- : Boolean flag indicating whether this is for an update operation

## Dependencies
- Functions called/Symbols referenced:
  - tupleLockExtraInfo (global array mapping lock modes to status info)
  - elog (error reporting)
- Type references:
  - [LockTupleMode](../L/LockTupleMode.md) (tuple locking mode enum)
  - [MultiXactStatus](../M/MultiXactStatus.md) (multi-transaction status enum)
- Called from (representative examples):
  - [heap_lock_tuple](../h/heap_lock_tuple.md) (tuple locking operations)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md) (computing new transaction info)
  - [test_lockmode_for_conflict](../t/test_lockmode_for_conflict.md) (testing lock conflicts)

## Notes and Other Information
- Part of PostgreSQL's multi-transaction (MultiXact) system for handling concurrent tuple locks
- Uses a lookup table approach for efficient mode-to-status mapping
- Validates input to prevent invalid lock mode/update flag combinations
- Critical for proper MultiXact member creation and conflict detection
- Supports both update and non-update lock operations with different status mappings
- Error handling ensures system consistency by rejecting invalid lock requests