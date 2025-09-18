# CheckPointPredicate

## Location
[src/backend/storage/lmgr/predicate.c:1041-1144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1041-L1144)

## Overview
CheckPointPredicate performs checkpoint operations for the predicate locking system, primarily focused on truncating the Serial SLRU to reclaim disk space for pages that are no longer needed.

## Definition
```c
void CheckPointPredicate(void)
```

## Detailed Description
This function is called during PostgreSQL checkpoints (both shutdown and on-the-fly) to maintain the Serial SLRU (Simple Least Recently Used cache) used for serializable transaction conflict detection. The function performs several key operations:

1. **Early Exit Check**: If the SLRU is not currently in use (headPage < 0), it exits immediately.

2. **Truncation Logic**: Determines the appropriate cutoff page for truncation based on the current state:
   - If tailXid is valid, it calculates the tail page and compares it with the head page
   - Uses the earlier of tailPage or headPage as the truncation cutoff to handle cases where tailXid is ahead of headXid
   - If tailXid is invalid, truncates to the head page and marks the SLRU as unused

3. **SLRU Maintenance**: Calls SimpleLruTruncate to remove obsolete pages and SimpleLruWriteAll to write dirty pages to disk.

The function includes detailed comments about edge cases related to XID wraparound and the interaction with VACUUM operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease
  - TransactionIdIsValid
  - SerialPage
  - SerialPagePrecedesLogically
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md)
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md)

## Notes and Other Information
- This is a public function accessible outside predicate.c
- Does not contain critical data that needs to survive restarts - primarily used for SLRU maintenance
- The function includes extensive comments about potential issues with XID wraparound scenarios
- Writing dirty pages to disk is done primarily as a debugging aid, not for correctness
- The truncation logic handles the complex case where in-progress serializable transactions may cause tailXid to be ahead of headXid
- Part of PostgreSQL's checkpoint infrastructure for maintaining serializable isolation system resources