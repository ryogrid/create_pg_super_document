# heap_prepare_freeze_tuple

## Location
[src/backend/access/heap/heapam.c:7009-7282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7009-L7282)

## Overview
Analyzes a tuple's transaction ID fields (xmin, xmax, xvac) to determine if freezing is needed and prepares a freeze plan that can be executed to freeze the tuple while maintaining MVCC consistency.

## Definition

```c
bool
heap_prepare_freeze_tuple(HeapTupleHeader tuple,
						  const struct VacuumCutoffs *cutoffs,
						  HeapPageFreeze *pagefrz,
						  HeapTupleFreeze *frz, bool *totally_frozen)
```
## Detailed Description
heap_prepare_freeze_tuple is a core component of PostgreSQL's tuple freezing mechanism, responsible for analyzing tuple headers and preparing freeze plans during VACUUM operations. The function examines all transaction ID fields in a tuple (xmin, xmax, xvac) against various age-based cutoffs to determine what freezing actions are needed.

The function implements sophisticated logic to:
1. Validate transaction IDs against corruption scenarios
2. Determine which fields need freezing based on cutoff thresholds
3. Handle complex MultiXactId scenarios through FreezeMultiXactId
4. Prepare detailed freeze plans with appropriate infomask modifications
5. Track whether tuples become totally frozen after processing
6. Coordinate page-level freezing requirements

The function returns true if a freeze plan was prepared, false if no action is needed. It ensures that the FreezeLimit and MultiXactCutoff postconditions are never violated while optimizing for performance by avoiding unnecessary work.

## Parameters
- : Pointer to the tuple header to analyze for freezing
- : Structure containing various vacuum cutoff thresholds (FreezeLimit, OldestXmin, etc.)
- : Input/output structure managing page-level freezing state and requirements
- : Output structure containing the prepared freeze plan for this tuple
- : Output parameter indicating if tuple will be completely frozen after plan execution

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetXvac
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md)
  - [GetMultiXactIdHintBits](../G/GetMultiXactIdHintBits.md)
  - [heap_tuple_should_freeze](heap_tuple_should_freeze.md)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - MultiXactIdIsValid
  - HEAP_XMAX_IS_MULTI
  - HEAP_XMAX_IS_LOCKED_ONLY
  - HEAP_MOVED_OFF
- Called from:
  - [heap_freeze_tuple](heap_freeze_tuple.md)
  - [heap_prune_record_unchanged_lp_normal](heap_prune_record_unchanged_lp_normal.md)
  - HeapScanIsValid (via header inclusion)

## Notes and Other Information
- **Return Value**: Returns true if any freeze plan was prepared, false if tuple needs no changes
- **Side Effects**: May allocate new MultiXactIds when processing complex xmax values
- **Freeze Plan Structure**: The output frz structure contains detailed instructions for tuple modification including new XID values and infomask changes
- **Page-Level Coordination**: Works with pagefrz to manage page-level freezing requirements and track various cutoff thresholds
- **Corruption Detection**: Includes extensive validation to detect and report data corruption scenarios
- **MultiXact Handling**: Delegates complex MultiXactId processing to FreezeMultiXactId while managing the integration of results
- **Total Freezing**: Tracks whether tuples become completely frozen (no remaining XIDs/MXIDs needing future processing)
- **Performance Optimization**: Designed to minimize unnecessary work while ensuring all freezing postconditions are met
- **MVCC Compliance**: Maintains MVCC semantics by carefully validating transaction states before freezing
- **Buffer Locking**: Caller must hold exclusive lock on shared buffers containing the tuple

## Simplified Source

```c
bool heap_prepare_freeze_tuple(HeapTupleHeader tuple,
                             const struct VacuumCutoffs *cutoffs,
                             HeapPageFreeze *pagefrz,
                             HeapTupleFreeze *frz, bool *totally_frozen) {
    bool xmin_already_frozen = false, xmax_already_frozen = false;
    bool freeze_xmin = false, replace_xvac = false;
    bool replace_xmax = false, freeze_xmax = false;
    TransactionId xid;

    // Initialize freeze plan
    frz->xmax = HeapTupleHeaderGetRawXmax(tuple);
    frz->t_infomask2 = tuple->t_infomask2;
    frz->t_infomask = tuple->t_infomask;
    frz->frzflags = 0;
    frz->checkflags = 0;

    // Process xmin field
    xid = HeapTupleHeaderGetXmin(tuple);
    if (!TransactionIdIsNormal(xid)) {
        xmin_already_frozen = true;
    } else {
        // Validate xmin against corruption
        if (TransactionIdPrecedes(xid, cutoffs->relfrozenxid))
            ereport(ERROR, "found xmin from before relfrozenxid");

        // Check if xmin needs freezing
        freeze_xmin = TransactionIdPrecedes(xid, cutoffs->OldestXmin);
        if (freeze_xmin)
            frz->checkflags |= HEAP_FREEZE_CHECK_XMIN_COMMITTED;
    }

    // Process xvac field (legacy VACUUM FULL support)
    xid = HeapTupleHeaderGetXvac(tuple);
    if (TransactionIdIsNormal(xid)) {
        // Always freeze xvac proactively
        replace_xvac = pagefrz->freeze_required = true;
    }

    // Process xmax field (most complex part)
    xid = frz->xmax;
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        // Handle MultiXactId case
        TransactionId newxmax;
        uint16 flags;

        newxmax = FreezeMultiXactId(xid, tuple->t_infomask, cutoffs, &flags, pagefrz);

        if (flags & FRM_NOOP) {
            // No changes needed for this MultiXactId
        } else if (flags & FRM_RETURN_IS_XID) {
            // Convert MultiXactId to simple XID
            frz->t_infomask &= ~HEAP_XMAX_BITS;
            frz->xmax = newxmax;
            if (flags & FRM_MARK_COMMITTED)
                frz->t_infomask |= HEAP_XMAX_COMMITTED;
            replace_xmax = true;
        } else if (flags & FRM_RETURN_IS_MULTI) {
            // Replace with new MultiXactId
            uint16 newbits, newbits2;
            frz->t_infomask &= ~HEAP_XMAX_BITS;
            frz->t_infomask2 &= ~HEAP_KEYS_UPDATED;
            GetMultiXactIdHintBits(newxmax, &newbits, &newbits2);
            frz->t_infomask |= newbits;
            frz->t_infomask2 |= newbits2;
            frz->xmax = newxmax;
            replace_xmax = true;
        } else {
            // Completely freeze xmax
            freeze_xmax = true;
        }
    } else if (TransactionIdIsNormal(xid)) {
        // Handle normal XID case
        if (TransactionIdPrecedes(xid, cutoffs->relfrozenxid))
            ereport(ERROR, "found xmax from before relfrozenxid");

        freeze_xmax = TransactionIdPrecedes(xid, cutoffs->OldestXmin);
        if (freeze_xmax && !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            frz->checkflags |= HEAP_FREEZE_CHECK_XMAX_ABORTED;
    } else if (!TransactionIdIsValid(xid)) {
        // Already frozen xmax
        xmax_already_frozen = true;
    }

    // Apply freeze plan flags
    if (freeze_xmin) {
        frz->t_infomask |= HEAP_XMIN_FROZEN;
    }
    if (replace_xvac) {
        if (tuple->t_infomask & HEAP_MOVED_OFF)
            frz->frzflags |= XLH_INVALID_XVAC;
        else
            frz->frzflags |= XLH_FREEZE_XVAC;
    }
    if (freeze_xmax) {
        frz->xmax = InvalidTransactionId;
        frz->t_infomask &= ~HEAP_XMAX_BITS;
        frz->t_infomask |= HEAP_XMAX_INVALID;
        frz->t_infomask2 &= ~(HEAP_HOT_UPDATED | HEAP_KEYS_UPDATED);
    }

    // Determine if tuple becomes totally frozen
    *totally_frozen = ((freeze_xmin || xmin_already_frozen) &&
                      (freeze_xmax || xmax_already_frozen));

    // Check if page-level freezing is required
    if (!pagefrz->freeze_required && !(xmin_already_frozen && xmax_already_frozen)) {
        pagefrz->freeze_required = heap_tuple_should_freeze(tuple, cutoffs,
                                                          &pagefrz->NoFreezePageRelfrozenXid,
                                                          &pagefrz->NoFreezePageRelminMxid);
    }

    // Return true if any freeze actions are planned
    return freeze_xmin || replace_xvac || replace_xmax || freeze_xmax;
}
```