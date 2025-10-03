# vac_truncate_clog

## Location
[src/backend/commands/vacuum.c:1804-1972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1804-L1972)

## Overview
Attempts to truncate transaction commit logs (pg_xact), commit timestamps, and MultiXact logs by scanning all databases to find the system-wide oldest datfrozenxid and datminmxid values.

## Definition

```c
static void
vac_truncate_clog(TransactionId frozenXID,
				  MultiXactId minMulti,
				  TransactionId lastSaneFrozenXid,
				  MultiXactId lastSaneMinMulti)
```
## Detailed Description
This function performs critical system maintenance by truncating various transaction-related logs when it's safe to do so. The process involves:

1. **Cluster-wide Locking**: Acquires WrapLimitsVacuumLock to ensure only one backend per cluster performs this operation
2. **Database Scanning**: Scans all pg_database entries to find the system-wide minimum datfrozenxid and datminmxid values
3. **Safety Validation**: Checks for wraparound conditions and "future" transaction IDs that indicate corruption
4. **Log Truncation**: Truncates pg_xact (CLOG), commit timestamps, and MultiXact logs based on the computed minimums
5. **Limit Updates**: Updates transaction ID wrap limits maintained by varsup.c to prevent wraparound

The function implements multiple safety mechanisms including detection of already-wrapped transactions and bogus data. It ensures that commit timestamp lookups return NULL rather than file errors for truncated transactions by advancing the oldest commit timestamp XID before truncation.

## Parameters / Member Variables
- `frozenXID`: The updated datfrozenxid value for the current database, used to initialize minimum calculations
- `minMulti`: The updated datminmxid value for the current database, used to initialize minimum calculations
- `lastSaneFrozenXid`: The latest valid frozen XID that could be seen during the scan (used for corruption detection)
- `lastSaneMinMulti`: The latest valid minimum MultiXactId that could be seen during the scan (used for corruption detection)
## Dependencies
- Functions called/Symbols referenced:
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - TransactionIdIsNormal
  - MultiXactIdIsValid
  - [database_is_invalid_form](../d/database_is_invalid_form.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [table_endscan](../t/table_endscan.md)
  - [AdvanceOldestCommitTsXid](../A/AdvanceOldestCommitTsXid.md)
  - [TruncateCLOG](../T/TruncateCLOG.md)
  - [TruncateCommitTs](../T/TruncateCommitTs.md)
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - [SetTransactionIdLimit](../S/SetTransactionIdLimit.md)
  - [SetMultiXactIdLimit](../S/SetMultiXactIdLimit.md)
- Called from (representative examples):
  - [vac_update_datfrozenxid](vac_update_datfrozenxid.md)

## Notes and Other Information
- This is a static function, only called from within the same source file
- Uses exclusive locking (WrapLimitsVacuumLock) to prevent concurrent truncation operations across the cluster
- Implements a "chicken out" strategy when detecting potentially corrupt data ("future" transaction IDs)
- Skips invalid databases that are in the process of being dropped or have been interrupted during dropping
- Issues warnings for potential transaction wraparound scenarios but continues safely
- The function assumes that fetching/updating XIDs in shared storage is atomic
- Handles race conditions gracefully - concurrent VACUUM operations at worst case result in less aggressive truncation
- Updates wrap limits for both transaction IDs and MultiXactIds, which may signal the postmaster for additional autovacuum cycles
- Advances commit timestamp tracking before truncation to provide better user experience (NULL instead of file errors)

## Simplified Source

```c
static void
vac_truncate_clog(TransactionId frozenXID,
                  MultiXactId minMulti,
                  TransactionId lastSaneFrozenXid,
                  MultiXactId lastSaneMinMulti)
{
    TransactionId nextXID = ReadNextTransactionId();
    Relation relation;
    TableScanDesc scan;
    HeapTuple tuple;
    Oid oldestxid_datoid, minmulti_datoid;
    bool bogus = false;
    bool frozenAlreadyWrapped = false;

    // Acquire exclusive lock to ensure only one backend truncates CLOG
    LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);

    // Initialize database IDs to current database
    oldestxid_datoid = MyDatabaseId;
    minmulti_datoid = MyDatabaseId;

    // Scan pg_database to find system-wide minimum frozen XIDs
    relation = table_open(DatabaseRelationId, AccessShareLock);
    scan = table_beginscan_catalog(relation, 0, NULL);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_database dbform = (Form_pg_database) GETSTRUCT(tuple);
        TransactionId datfrozenxid = dbform->datfrozenxid;
        TransactionId datminmxid = dbform->datminmxid;

        // Skip invalid/dropping databases
        if (database_is_invalid_form(dbform))
            continue;

        // Check for corruption (future XIDs indicate bugs)
        if (TransactionIdPrecedes(lastSaneFrozenXid, datfrozenxid) ||
            MultiXactIdPrecedes(lastSaneMinMulti, datminmxid))
            bogus = true;

        // Check for wraparound condition
        if (TransactionIdPrecedes(nextXID, datfrozenxid))
            frozenAlreadyWrapped = true;
        // Update minimum frozen XID
        else if (TransactionIdPrecedes(datfrozenxid, frozenXID))
        {
            frozenXID = datfrozenxid;
            oldestxid_datoid = dbform->oid;
        }

        // Update minimum MultiXact ID
        if (MultiXactIdPrecedes(datminmxid, minMulti))
        {
            minMulti = datminmxid;
            minmulti_datoid = dbform->oid;
        }
    }

    table_endscan(scan);
    table_close(relation, AccessShareLock);

    // Abort if wraparound already occurred
    if (frozenAlreadyWrapped)
    {
        ereport(WARNING, (errmsg("some databases have not been vacuumed in over 2 billion transactions")));
        LWLockRelease(WrapLimitsVacuumLock);
        return;
    }

    // Abort if corruption detected
    if (bogus)
    {
        LWLockRelease(WrapLimitsVacuumLock);
        return;
    }

    // Advance commit timestamp tracking before truncation
    AdvanceOldestCommitTsXid(frozenXID);

    // Truncate transaction logs to computed minimums
    TruncateCLOG(frozenXID, oldestxid_datoid);
    TruncateCommitTs(frozenXID);
    TruncateMultiXact(minMulti, minmulti_datoid);

    // Update wrap limits for future transactions
    SetTransactionIdLimit(frozenXID, oldestxid_datoid);
    SetMultiXactIdLimit(minMulti, minmulti_datoid, false);

    LWLockRelease(WrapLimitsVacuumLock);
}
```