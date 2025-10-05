# StandbyAcquireAccessExclusiveLock

## Location
[src/backend/storage/ipc/standby.c:985-1033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L985-L1033)

## Overview
StandbyAcquireAccessExclusiveLock acquires AccessExclusive locks during recovery replay to maintain consistency between the primary and standby servers in PostgreSQL's hot standby mode.

## Definition
void StandbyAcquireAccessExclusiveLock(TransactionId xid, Oid dbOid, Oid relOid)

## Detailed Description
This function is a core component of PostgreSQL's hot standby locking mechanism. It acquires AccessExclusive locks during WAL replay to ensure that standby queries cannot access relations that were exclusively locked on the primary server. The function manages lock state through hash tables (RecoveryLockHash and RecoveryLockXidHash) to efficiently track and deduplicate locks reported in checkpoints and WAL records. All locks are held by the Startup process using a single virtual transaction, acting as a proxy for the original transactions that held these locks on the primary.

## Parameters / Member Variables
- : Transaction ID that originally held the lock on the primary server
- : Database OID of the relation (InvalidOid for shared relations)
- : Relation OID of the table/index being locked

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [TransactionIdDidAbort](../T/TransactionIdDidAbort.md)
  - [hash_search](../h/hash_search.md)
  - SET_LOCKTAG_RELATION
  - [LockAcquire](../L/LockAcquire.md)
  - [RecoveryLockXidEntry](../R/RecoveryLockXidEntry.md)
  - [RecoveryLockEntry](../R/RecoveryLockEntry.md)
  - [xl_standby_lock](../x/xl_standby_lock.md)
  - [LOCKTAG](../L/LOCKTAG.md)
- Called from (representative examples):
  - [standby_redo](../s/standby_redo.md) (src/backend/storage/ipc/standby.c:1176)
  - [lock_twophase_standby_recover](../l/lock_twophase_standby_recover.md) (src/backend/storage/lmgr/lock.c:4374)

## Notes and Other Information
- Only tracks AccessExclusive locks, which are held by one transaction on one relation
- Uses session locks rather than normal locks to avoid needing ResourceOwners
- Performs deduplication to handle checkpoint re-reporting of existing locks
- Skips processing for invalid, committed, or aborted transactions
- [Hash](../H/Hash.md) table entries link locks to their original transaction IDs for efficient cleanup
- Part of the recovery locking infrastructure that prevents query conflicts in hot standby mode

## Simplified Source

```c
void StandbyAcquireAccessExclusiveLock(TransactionId xid, Oid dbOid, Oid relOid) {
    RecoveryLockXidEntry *xidentry;
    RecoveryLockEntry *lockentry;
    xl_standby_lock key;
    LOCKTAG locktag;
    bool found;

    // Skip if transaction is invalid, committed, or aborted
    if (!TransactionIdIsValid(xid) ||
        TransactionIdDidCommit(xid) ||
        TransactionIdDidAbort(xid))
        return;

    elog(DEBUG4, "adding recovery lock: db %u rel %u", dbOid, relOid);
    Assert(OidIsValid(relOid));

    // Create/find hash entry for this transaction ID
    xidentry = hash_search(RecoveryLockXidHash, &xid, HASH_ENTER, &found);
    if (!found) {
        Assert(xidentry->xid == xid);
        xidentry->head = NULL;
    }

    // Create/find hash entry for this specific lock
    key.xid = xid;
    key.dbOid = dbOid;
    key.relOid = relOid;
    lockentry = hash_search(RecoveryLockHash, &key, HASH_ENTER, &found);

    if (!found) {
        // New lock - link it to transaction's lock list
        lockentry->next = xidentry->head;
        xidentry->head = lockentry;

        // Acquire the AccessExclusive lock locally
        SET_LOCKTAG_RELATION(locktag, dbOid, relOid);
        (void) LockAcquire(&locktag, AccessExclusiveLock, true, false);
    }
    // If lock already exists, deduplication - no action needed
}
```