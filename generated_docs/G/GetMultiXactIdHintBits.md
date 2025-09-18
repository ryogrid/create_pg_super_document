# GetMultiXactIdHintBits

## Location
src/backend/access/heap/heapam.c: 7425 - 7505

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
  - GetMultiXactIdMembers
  - TUPLOCK_from_mxstatus
  - pfree
- Types used:
  - MultiXactId
  - MultiXactMember
  - LockTupleMode
- Constants used:
  - HEAP_XMAX_IS_MULTI
  - HEAP_XMAX_EXCL_LOCK
  - HEAP_XMAX_SHR_LOCK
  - HEAP_XMAX_KEYSHR_LOCK
  - HEAP_XMAX_LOCK_ONLY
  - HEAP_KEYS_UPDATED
  - LockTupleKeyShare, LockTupleShare, LockTupleExclusive, LockTupleNoKeyExclusive
  - MultiXactStatus values
- Called from (representative examples):
  - heap_update
  - compute_new_xmax_infomask
  - heap_prepare_freeze_tuple

## Notes and Other Information
- Static function, only used within heapam.c
- Optimized for newly created MultiXactIds that are in local cache
- Does not handle pre-pg_upgrade MultiXactId values
- Always sets HEAP_XMAX_IS_MULTI bit since it's dealing with a MultiXactId
- Tracks strongest lock mode and presence of updates to determine appropriate hint bits
- Frees the members array obtained from GetMultiXactIdMembers