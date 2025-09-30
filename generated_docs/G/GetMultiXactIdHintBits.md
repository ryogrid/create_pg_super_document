# GetMultiXactIdHintBits

## Location
[src/backend/access/heap/heapam.c:7425-7505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7425-L7505)

## Overview
Static function that determines the appropriate hint bits to set in a tuple's infomask based on the members and lock modes of a given MultiXactId.

## Definition
```c
static void GetMultiXactIdHintBits(MultiXactId multi, uint16 *new_infomask, uint16 *new_infomask2)
```

## Detailed Description
This function analyzes a MultiXactId to determine what hint bits should be set in a heap tuple's infomask and infomask2 fields. It retrieves the members of the MultiXactId and examines their lock statuses to determine the strongest lock mode held and whether any updates are present. Based on this analysis, it sets appropriate bits like HEAP_XMAX_IS_MULTI, HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_SHR_LOCK, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY, and HEAP_KEYS_UPDATED.

The function is typically called for newly created MultiXactIds that are expected to be in the local cache, making the GetMultiXactIdMembers call fast. It processes each member's status to determine the overall characteristics of the MultiXactId.

## Parameters / Member Variables
- `multi`: The MultiXactId to analyze
- `new_infomask`: Output parameter for the new infomask bits to be set
- `new_infomask2`: Output parameter for the new infomask2 bits to be set

## Dependencies
- Functions called/Symbols referenced:
  - [GetMultiXactIdMembers](GetMultiXactIdMembers.md)
  - TUPLOCK_from_mxstatus
  - [pfree](../p/pfree.md)
- Types used:
  - MultiXactId
  - [MultiXactMember](../M/MultiXactMember.md)
  - [LockTupleMode](../L/LockTupleMode.md)
- Constants used:
  - HEAP_XMAX_IS_MULTI
  - HEAP_XMAX_EXCL_LOCK
  - HEAP_XMAX_SHR_LOCK
  - HEAP_XMAX_KEYSHR_LOCK
  - HEAP_XMAX_LOCK_ONLY
  - HEAP_KEYS_UPDATED
  - LockTupleKeyShare, LockTupleShare, LockTupleExclusive, LockTupleNoKeyExclusive
  - [MultiXactStatus](../M/MultiXactStatus.md) values
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)

## Notes and Other Information
- Static function, only used within heapam.c
- Optimized for newly created MultiXactIds that are in local cache
- Does not handle pre-pg_upgrade MultiXactId values
- Always sets HEAP_XMAX_IS_MULTI bit since it's dealing with a MultiXactId
- Tracks strongest lock mode and presence of updates to determine appropriate hint bits
- Frees the members array obtained from GetMultiXactIdMembers

## Simplified Source

```c
static void GetMultiXactIdHintBits(MultiXactId multi, uint16 *new_infomask, uint16 *new_infomask2) {
    MultiXactMember *members;
    int nmembers;
    uint16 bits = HEAP_XMAX_IS_MULTI;
    uint16 bits2 = 0;
    bool has_update = false;
    LockTupleMode strongest = LockTupleKeyShare;

    // Get all members of this MultiXactId
    nmembers = GetMultiXactIdMembers(multi, &members, false, false);

    // Process each member to determine lock characteristics
    for (int i = 0; i < nmembers; i++) {
        LockTupleMode mode = TUPLOCK_from_mxstatus(members[i].status);

        // Track the strongest lock mode
        if (mode > strongest)
            strongest = mode;

        // Set appropriate bits based on member status
        switch (members[i].status) {
            case MultiXactStatusForUpdate:
            case MultiXactStatusUpdate:
                bits2 |= HEAP_KEYS_UPDATED;
                if (members[i].status == MultiXactStatusUpdate)
                    has_update = true;
                break;
            case MultiXactStatusNoKeyUpdate:
                has_update = true;
                break;
        }
    }

    // Set lock type bits based on strongest lock mode
    if (strongest == LockTupleExclusive || strongest == LockTupleNoKeyExclusive)
        bits |= HEAP_XMAX_EXCL_LOCK;
    else if (strongest == LockTupleShare)
        bits |= HEAP_XMAX_SHR_LOCK;
    else if (strongest == LockTupleKeyShare)
        bits |= HEAP_XMAX_KEYSHR_LOCK;

    // Set lock-only bit if no updates present
    if (!has_update)
        bits |= HEAP_XMAX_LOCK_ONLY;

    // Clean up and return results
    if (nmembers > 0)
        pfree(members);

    *new_infomask = bits;
    *new_infomask2 = bits2;
}
```