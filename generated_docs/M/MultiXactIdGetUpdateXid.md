# MultiXactIdGetUpdateXid

## Location
src/backend/access/heap/heapam.c: 7506 - 7557

## Overview
Static function that extracts and returns the transaction ID of the updating transaction from a MultiXactId, given that the MultiXactId contains an update (not lock-only).

## Definition
```c
static TransactionId MultiXactIdGetUpdateXid(TransactionId xmax, uint16 t_infomask)
```

## Detailed Description
This function takes a MultiXactId (stored in xmax) and its corresponding infomask, and extracts the transaction ID of the updating transaction within the MultiXactId. It's designed to work with MultiXactIds that contain actual updates, not just locks (verified by checking that HEAP_XMAX_LOCK_ONLY is not set). The function iterates through all members of the MultiXactId and identifies the one that represents an update operation using ISUPDATE_from_mxstatus().

The function includes assertions to ensure there is at most one updating transaction within the MultiXactId, which is a fundamental invariant of PostgreSQL's MultiXactId design. In non-debug builds, it can break early after finding the updater, while in debug builds it continues to verify this invariant.

## Parameters / Member Variables
- `xmax`: The MultiXactId from which to extract the updating transaction ID
- `t_infomask`: The infomask bits associated with the tuple, used for validation

## Dependencies
- Functions called/Symbols referenced:
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - ISUPDATE_from_mxstatus
  - [pfree](../p/pfree.md)
- Types used:
  - TransactionId
  - [MultiXactMember](MultiXactMember.md)
- Constants used:
  - InvalidTransactionId
  - HEAP_XMAX_LOCK_ONLY
  - HEAP_XMAX_IS_MULTI
- Called from (representative examples):
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md)
  - [HeapTupleGetUpdateXid](../H/HeapTupleGetUpdateXid.md)

## Notes and Other Information
- Static function, only used within heapam.c
- Requires that HEAP_XMAX_LOCK_ONLY bit is not set in t_infomask
- Requires that HEAP_XMAX_IS_MULTI bit is set in t_infomask
- Does not handle pre-pg_upgrade MultiXactIds since LOCK_ONLY bit is not set
- Returns InvalidTransactionId if no updating transaction is found
- Includes assertion checking to verify at most one updater exists
- In debug builds, validates the entire MultiXactId to ensure only one updater
- Caller is responsible for checking the status of the returned transaction ID