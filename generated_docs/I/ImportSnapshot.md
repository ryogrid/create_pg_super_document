# ImportSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1367-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1367-L1553)

## Overview
ImportSnapshot loads a previously exported snapshot from a file and sets it as the current transaction snapshot, enabling snapshot sharing between transactions.

## Definition
```c
void ImportSnapshot(const char *idstr)
```

## Detailed Description
ImportSnapshot implements PostgreSQL's snapshot import functionality, allowing transactions to adopt a previously exported snapshot state. This function performs extensive validation to ensure snapshot compatibility and consistency:

1. **Transaction State Validation**: Ensures the function is called at the top level of a fresh transaction without any assigned XID or subtransactions
2. **Isolation Level Compatibility**: Requires SERIALIZABLE or REPEATABLE READ isolation levels
3. **File Security**: Validates the snapshot identifier format to prevent arbitrary file access
4. **Snapshot Parsing**: Reads and parses the snapshot file containing transaction visibility information
5. **Cross-Database Protection**: Prevents importing snapshots from different databases to maintain vacuum consistency
6. **Serializable Transaction Constraints**: Enforces additional restrictions for serializable transactions

The function reads snapshot data from files stored in SNAPSHOT_EXPORT_DIR, parsing various fields including transaction IDs, isolation levels, and visibility arrays.

## Parameters / Member Variables
- `idstr`: The snapshot identifier/filename to import from SNAPSHOT_EXPORT_DIR (must contain only 0-9, A-F, and hyphens)

## Dependencies
- Functions called/Symbols referenced:
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - [IsSubTransaction](IsSubTransaction.md)
  - IsolationUsesXactSnapshot
  - [AllocateFile](../A/AllocateFile.md)
  - [parseVxidFromText](../p/parseVxidFromText.md)
  - [parseIntFromText](../p/parseIntFromText.md)
  - [parseXidFromText](../p/parseXidFromText.md)
  - [SetTransactionSnapshot](../S/SetTransactionSnapshot.md)
  - VirtualTransactionIdIsValid
  - TransactionIdIsNormal
  - IsolationIsSerializable
- Called from (representative examples):
  - [ExecSetVariableStmt](../E/ExecSetVariableStmt.md) (for SET TRANSACTION SNAPSHOT command)

## Notes and Other Information
- Must be called before any query execution in a transaction
- Only works with SERIALIZABLE or REPEATABLE READ isolation levels
- Cannot import snapshots from different databases due to vacuum consistency requirements
- Serializable transactions have additional constraints regarding read-only status compatibility
- File format includes metadata like source vxid, pid, database ID, and isolation level
- Performs extensive validation to prevent security issues and maintain transaction consistency

## Simplified Source

```c
void
ImportSnapshot(const char *idstr)
{
    char path[MAXPGPATH];
    FILE *f;
    struct stat stat_buf;
    char *filebuf;
    VirtualTransactionId src_vxid;
    int src_pid, src_isolevel;
    Oid src_dbid;
    bool src_readonly;
    SnapshotData snapshot;

    // Validate transaction state - must be at top level with no XID
    if (FirstSnapshotSet ||
        GetTopTransactionIdIfAny() != InvalidTransactionId ||
        IsSubTransaction())
        ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                errmsg("SET TRANSACTION SNAPSHOT must be called before any query")));

    // Check isolation level compatibility
    if (!IsolationUsesXactSnapshot())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ")));

    // Validate snapshot identifier format for security
    if (strspn(idstr, "0123456789ABCDEF-") != strlen(idstr))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid snapshot identifier: \"%s\"", idstr)));

    // Read snapshot file
    snprintf(path, MAXPGPATH, SNAPSHOT_EXPORT_DIR "/%s", idstr);
    f = AllocateFile(path, PG_BINARY_R);
    if (!f) {
        if (errno == ENOENT)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("snapshot \"%s\" does not exist", idstr)));
        else
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\" for reading: %m", path)));
    }

    // Load file content
    if (fstat(fileno(f), &stat_buf))
        elog(ERROR, "could not stat file \"%s\": %m", path);

    filebuf = (char *) palloc(stat_buf.st_size + 1);
    if (fread(filebuf, stat_buf.st_size, 1, f) != 1)
        elog(ERROR, "could not read file \"%s\": %m", path);
    filebuf[stat_buf.st_size] = '\0';
    FreeFile(f);

    // Parse snapshot metadata
    memset(&snapshot, 0, sizeof(snapshot));
    parseVxidFromText("vxid:", &filebuf, path, &src_vxid);
    src_pid = parseIntFromText("pid:", &filebuf, path);
    src_dbid = parseXidFromText("dbid:", &filebuf, path);
    src_isolevel = parseIntFromText("iso:", &filebuf, path);
    src_readonly = parseIntFromText("ro:", &filebuf, path);

    // Parse core snapshot data
    snapshot.snapshot_type = SNAPSHOT_MVCC;
    snapshot.xmin = parseXidFromText("xmin:", &filebuf, path);
    snapshot.xmax = parseXidFromText("xmax:", &filebuf, path);
    snapshot.xcnt = parseIntFromText("xcnt:", &filebuf, path);

    // Parse transaction arrays with validation
    if (snapshot.xcnt < 0 || snapshot.xcnt > GetMaxSnapshotXidCount())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid snapshot data in file \"%s\"", path)));

    snapshot.xip = (TransactionId *) palloc(snapshot.xcnt * sizeof(TransactionId));
    for (int i = 0; i < snapshot.xcnt; i++)
        snapshot.xip[i] = parseXidFromText("xip:", &filebuf, path);

    snapshot.suboverflowed = parseIntFromText("sof:", &filebuf, path);
    if (!snapshot.suboverflowed) {
        snapshot.subxcnt = parseIntFromText("sxcnt:", &filebuf, path);
        if (snapshot.subxcnt < 0 || snapshot.subxcnt > GetMaxSnapshotSubxidCount())
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid snapshot data in file \"%s\"", path)));

        snapshot.subxip = (TransactionId *) palloc(snapshot.subxcnt * sizeof(TransactionId));
        for (int i = 0; i < snapshot.subxcnt; i++)
            snapshot.subxip[i] = parseXidFromText("sxp:", &filebuf, path);
    } else {
        snapshot.subxcnt = 0;
        snapshot.subxip = NULL;
    }

    snapshot.takenDuringRecovery = parseIntFromText("rec:", &filebuf, path);

    // Validate snapshot consistency
    if (!VirtualTransactionIdIsValid(src_vxid) ||
        !OidIsValid(src_dbid) ||
        !TransactionIdIsNormal(snapshot.xmin) ||
        !TransactionIdIsNormal(snapshot.xmax))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid snapshot data in file \"%s\"", path)));

    // Check serializable transaction compatibility
    if (IsolationIsSerializable()) {
        if (src_isolevel != XACT_SERIALIZABLE)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("a serializable transaction cannot import a snapshot from a non-serializable transaction")));
        if (src_readonly && !XactReadOnly)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("a non-read-only serializable transaction cannot import a snapshot from a read-only transaction")));
    }

    // Prevent cross-database imports
    if (src_dbid != MyDatabaseId)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot import a snapshot from a different database")));

    // Install the snapshot
    SetTransactionSnapshot(&snapshot, &src_vxid, src_pid, NULL);
}
```