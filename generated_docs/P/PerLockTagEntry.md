# PerLockTagEntry

## Location
src/backend/storage/lmgr/lock.c: 3223 - 3303

## Overview
PerLockTagEntry is a local structure used to track whether locks on the same lockable object are held at both session and transaction levels during PREPARE TRANSACTION validation.

## Definition
```c
typedef struct
{
    LOCKTAG    lock;        /* identifies the lockable object */
    bool       sessLock;    /* is any lockmode held at session level? */
    bool       xactLock;    /* is any lockmode held at xact level? */
} PerLockTagEntry;
```

## Detailed Description
PerLockTagEntry is a temporary hash table entry structure used exclusively within the CheckForSessionAndXactLocks() function during PREPARE TRANSACTION processing. It serves to detect conflicts between session-level and transaction-level locks on the same lockable object.

The structure is used to build a transient hash table that aggregates lock information by LOCKTAG, consolidating multiple LOCALLOCK entries that may exist for different lock modes on the same object. This consolidation is necessary because the local lock table stores separate entries for each lock mode, making it impossible to detect session/transaction conflicts by examining individual LOCALLOCK entries.

When both sessLock and xactLock flags are true for the same LOCKTAG, it indicates an illegal state for PREPARE TRANSACTION, which must fail to ensure consistency in two-phase commit scenarios.

## Parameters / Member Variables
- `lock`: LOCKTAG that uniquely identifies the lockable object (relation, tuple, etc.)
- `sessLock`: Boolean flag indicating whether any lock mode is held at the session level on this object
- `xactLock`: Boolean flag indicating whether any lock mode is held at the transaction level on this object

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG (as member type)
  - [hash_search](../h/hash_search.md) (for hash table operations)
  - [hash_create](../h/hash_create.md) (for creating temporary hash table)
  - HASH_ENTER (hash operation flag)
- Called from (representative examples):
  - CheckForSessionAndXactLocks (only usage context)

## Notes and Other Information
- This is a local typedef structure, not a global type
- Only used within CheckForSessionAndXactLocks() function during PREPARE TRANSACTION
- The structure exists temporarily in a local hash table that is destroyed after validation
- Virtual transaction (VXID) locks are explicitly ignored as they are not meaningful after restart
- The detection of both session and transaction locks on the same object causes PREPARE TRANSACTION to fail with ERRCODE_FEATURE_NOT_SUPPORTED