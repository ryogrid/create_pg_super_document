# CoarserLockCovers

## Location
src/backend/storage/lmgr/predicate.c: 2101 - 2129

## Overview
Checks whether a lock target is already covered by an existing coarser granularity predicate lock held by the current transaction.

## Definition

```c
static bool
CoarserLockCovers(const PREDICATELOCKTARGETTAG *newtargettag)
```
## Detailed Description
This function determines whether acquiring a new predicate lock would be redundant because a coarser granularity lock that covers the target is already held by the transaction. It traverses up the lock hierarchy starting from the given target, checking each parent level to see if a lock exists at that granularity.

The function iteratively calls GetParentPredicateLockTag to walk up the hierarchy (from tuple to page to relation) and uses PredicateLockExists to check if a lock is held at each level. If any parent lock is found, the function returns true, indicating the new lock would be covered by the existing coarser lock.

Like PredicateLockExists, this function may return false negatives due to the local lock table limitations, but it will never return false positives, ensuring safety in lock acquisition decisions.

## Parameters / Member Variables
- : A pointer to the predicate lock target tag to check for coverage by coarser locks

## Dependencies
- Functions called/Symbols referenced:
  - GetParentPredicateLockTag
  - PredicateLockExists
- Called from (representative examples):
  - SerialControl
  - PredicateLockAcquire

## Notes and Other Information
- Static function, only accessible within predicate.c
- May return false negatives but never false positives
- Prevents redundant lock acquisition when coarser locks provide coverage
- Essential for optimizing the predicate locking system by avoiding unnecessary fine-grained locks
- Traverses the complete lock hierarchy from the target up to relation level
- Part of PostgreSQL's serializable snapshot isolation optimization