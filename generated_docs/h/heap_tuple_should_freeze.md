# heap_tuple_should_freeze

## Location
[src/backend/access/heap/heapam.c:7842-7949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7842-L7949)

## Overview
Determines whether a heap tuple should be frozen by checking if its transaction IDs (xmin, xmax, xvac) and MultiXact IDs are older than the freeze cutoff limits.

## Definition

```c
bool
heap_tuple_should_freeze(HeapTupleHeader tuple,
						 const struct VacuumCutoffs *cutoffs,
						 TransactionId *NoFreezePageRelfrozenXid,
						 MultiXactId *NoFreezePageRelminMxid)
```
## Detailed Description
This function serves as a sibling to heap_prepare_freeze_tuple and determines whether a tuple would (or should) force freezing of the heap page containing it. The function examines all transaction IDs and MultiXact IDs in the tuple header (xmin, xmax, xvac fields) against the provided freeze limits. If any XID/MXID is older than the corresponding cutoff (FreezeLimit/MultiXactCutoff), the function returns true indicating the tuple should be frozen.

The function also tracks the oldest extant XIDs/MXIDs remaining in the relation through the NoFreezePageRelfrozenXid and NoFreezePageRelminMxid parameters, which help VACUUM maintain accurate tracking of unfrozen tuples. The working assumption is that the caller won't freeze this tuple, so these trackers are only updated if the tuple contains older XIDs/MXIDs.

The function handles several special cases:
- MultiXact XIDs that may contain updater XIDs requiring individual member examination
- pg_upgrade'd MultiXacts (HEAP_LOCKED_UPGRADED) which are always frozen
- HEAP_MOVED tuples with xvac fields that are always frozen if they contain normal XIDs

## Parameters / Member Variables
- `tuple`: HeapTupleHeader to examine for freeze necessity
- `*cutoffs`: VacuumCutoffs structure containing freeze limits (FreezeLimit, MultiXactCutoff, etc.)
- `*NoFreezePageRelfrozenXid`: Input/output parameter tracking oldest unfrozen XID in relation
- `*NoFreezePageRelminMxid`: Input/output parameter tracking oldest unfrozen MultiXact ID in relation
## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetXvac
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [MultiXactIdPrecedesOrEquals](../M/MultiXactIdPrecedesOrEquals.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - HEAP_XMAX_IS_MULTI
  - HEAP_LOCKED_UPGRADED
  - HEAP_XMAX_IS_LOCKED_ONLY
  - HEAP_MOVED
- Called from (representative examples):
  - [heap_prepare_freeze_tuple](heap_prepare_freeze_tuple.md)
  - [lazy_scan_noprune](../l/lazy_scan_noprune.md)

## Notes and Other Information
- The function works in conjunction with heap_prepare_freeze_tuple, providing a way to determine freeze necessity without actually performing the freeze operation
- Used extensively by VACUUM operations to decide whether pages need freezing
- The NoFreezePageRelfrozenXid and NoFreezePageRelminMxid parameters are updated only when the assumption is that the tuple won't be frozen
- pg_upgrade'd MultiXacts are always considered for freezing regardless of their age
- xvac fields in HEAP_MOVED tuples always trigger freezing when they contain normal transaction IDs

## Simplified Source

```c
bool
heap_tuple_should_freeze(HeapTupleHeader tuple,
                        const struct VacuumCutoffs *cutoffs,
                        TransactionId *NoFreezePageRelfrozenXid,
                        MultiXactId *NoFreezePageRelminMxid)
{
    TransactionId xid;
    MultiXactId multi;
    bool freeze = false;

    // Check xmin
    xid = HeapTupleHeaderGetXmin(tuple);
    if (TransactionIdIsNormal(xid))
    {
        if (TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid))
            *NoFreezePageRelfrozenXid = xid;
        if (TransactionIdPrecedes(xid, cutoffs->FreezeLimit))
            freeze = true;
    }

    // Check xmax (either XID or MultiXactId)
    xid = InvalidTransactionId;
    multi = InvalidMultiXactId;

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
        multi = HeapTupleHeaderGetRawXmax(tuple);
    else
        xid = HeapTupleHeaderGetRawXmax(tuple);

    if (TransactionIdIsNormal(xid))
    {
        // Simple XID case
        if (TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid))
            *NoFreezePageRelfrozenXid = xid;
        if (TransactionIdPrecedes(xid, cutoffs->FreezeLimit))
            freeze = true;
    }
    else if (MultiXactIdIsValid(multi))
    {
        if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
        {
            // pg_upgrade'd MultiXact always needs freezing
            if (MultiXactIdPrecedes(multi, *NoFreezePageRelminMxid))
                *NoFreezePageRelminMxid = multi;
            freeze = true;
        }
        else
        {
            // Regular MultiXactId - check age and members
            if (MultiXactIdPrecedes(multi, *NoFreezePageRelminMxid))
                *NoFreezePageRelminMxid = multi;
            if (MultiXactIdPrecedes(multi, cutoffs->MultiXactCutoff))
                freeze = true;

            // Check individual member XIDs
            MultiXactMember *members;
            int nmembers = GetMultiXactIdMembers(multi, &members, false,
                                               HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

            for (int i = 0; i < nmembers; i++)
            {
                xid = members[i].xid;
                if (TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid))
                    *NoFreezePageRelfrozenXid = xid;
                if (TransactionIdPrecedes(xid, cutoffs->FreezeLimit))
                    freeze = true;
            }

            if (nmembers > 0)
                pfree(members);
        }
    }

    // Check xvac for HEAP_MOVED tuples
    if (tuple->t_infomask & HEAP_MOVED)
    {
        xid = HeapTupleHeaderGetXvac(tuple);
        if (TransactionIdIsNormal(xid))
        {
            if (TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid))
                *NoFreezePageRelfrozenXid = xid;
            freeze = true;  // Always freeze HEAP_MOVED with normal xvac
        }
    }

    return freeze;
}
```