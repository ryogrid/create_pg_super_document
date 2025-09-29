# ExportSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1095-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1095-L1271)

## Overview
ExportSnapshot exports a transaction snapshot to a file so that other backends can import and use it, enabling snapshot sharing across different database sessions.

## Definition
```c
char *ExportSnapshot(Snapshot snapshot)
```

## Detailed Description
ExportSnapshot creates a persistent file representation of a PostgreSQL transaction snapshot that can be imported by other database sessions. The function performs several key operations:

1. **Transaction Validation**: Ensures the snapshot is not being exported from a subtransaction, as this would be unreliable for importing sessions
2. **Snapshot Augmentation**: Adds the current transaction ID and any committed child transaction IDs to the snapshot data
3. **File Generation**: Creates a uniquely named snapshot file in the designated export directory using the format `procNumber-lxid-sequenceNumber`
4. **Serialization**: Converts the snapshot data into a structured text format with specific field:value pairs expected by ImportSnapshot
5. **Registration**: Registers the snapshot to ensure its xmin is honored for the remainder of the transaction

The exported snapshot file contains critical transaction information including virtual transaction ID, process ID, database ID, isolation level, read-only status, and the complete set of active and subactive transaction IDs.

## Parameters / Member Variables
- `snapshot`: The Snapshot structure to be exported containing transaction visibility information

## Dependencies
- Functions called/Symbols referenced:
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - [IsSubTransaction](../I/IsSubTransaction.md)  
  - [xactGetCommittedChildren](../x/xactGetCommittedChildren.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [CopySnapshot](../C/CopySnapshot.md)
  - [GetMaxSnapshotSubxidCount](../G/GetMaxSnapshotSubxidCount.md)
  - [AllocateFile](../A/AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)
  - [pairingheap_add](../p/pairingheap_add.md)
- Called from (representative examples):
  - [pg_export_snapshot](../p/pg_export_snapshot.md)
  - [SnapBuildExportSnapshot](../S/SnapBuildExportSnapshot.md)

## Notes and Other Information
- Cannot be called from within a subtransaction due to verification complexities
- The function includes committed child subtransactions in the exported snapshot
- File creation uses atomic rename operations (.tmp suffix) to prevent partial reads
- The returned token is the basename of the created file, used for importing the snapshot
- Snapshots are stored in TopTransactionContext and registered to maintain xmin visibility
- No fsync is performed as the snapshot file does not need to survive system crashes

## Simplified Source

```c
// Simplified version of ExportSnapshot
char *ExportSnapshot(Snapshot snapshot) {
    TransactionId topXid;
    TransactionId *children;
    ExportedSnapshot *exportedSnap;
    int numChildren;
    StringInfoData buffer;
    FILE *file;
    char filePath[MAXPGPATH];
    char tempPath[MAXPGPATH];

    // Get current transaction ID
    topXid = GetTopTransactionIdIfAny();

    // Validate not in subtransaction
    if (IsSubTransaction()) {
        ereport(ERROR, "cannot export a snapshot from a subtransaction");
    }

    // Get committed child transaction IDs
    numChildren = xactGetCommittedChildren(&children);

    // Generate unique snapshot file path
    snprintf(filePath, sizeof(filePath), SNAPSHOT_EXPORT_DIR "/%08X-%08X-%d",
             MyProc->vxid.procNumber, MyProc->vxid.lxid,
             list_length(exportedSnapshots) + 1);

    // Copy snapshot and register it in transaction context
    snapshot = CopySnapshot(snapshot);
    exportedSnap = palloc(sizeof(ExportedSnapshot));
    exportedSnap->snapfile = pstrdup(filePath);
    exportedSnap->snapshot = snapshot;
    exportedSnapshots = lappend(exportedSnapshots, exportedSnap);

    // Register snapshot to maintain xmin visibility
    snapshot->regd_count++;
    pairingheap_add(&RegisteredSnapshots, &snapshot->ph_node);

    // Build snapshot text representation
    initStringInfo(&buffer);

    // Add transaction metadata
    appendStringInfo(&buffer, "vxid:%d/%u\\n", MyProc->vxid.procNumber, MyProc->vxid.lxid);
    appendStringInfo(&buffer, "pid:%d\\n", MyProcPid);
    appendStringInfo(&buffer, "dbid:%u\\n", MyDatabaseId);
    appendStringInfo(&buffer, "iso:%d\\n", XactIsoLevel);
    appendStringInfo(&buffer, "ro:%d\\n", XactReadOnly);

    // Add snapshot bounds
    appendStringInfo(&buffer, "xmin:%u\\n", snapshot->xmin);
    appendStringInfo(&buffer, "xmax:%u\\n", snapshot->xmax);

    // Add active transaction IDs including our own if valid
    int shouldAddTopXid = (TransactionIdIsValid(topXid) &&
                          TransactionIdPrecedes(topXid, snapshot->xmax)) ? 1 : 0;
    appendStringInfo(&buffer, "xcnt:%d\\n", snapshot->xcnt + shouldAddTopXid);

    for (int i = 0; i < snapshot->xcnt; i++) {
        appendStringInfo(&buffer, "xip:%u\\n", snapshot->xip[i]);
    }
    if (shouldAddTopXid) {
        appendStringInfo(&buffer, "xip:%u\\n", topXid);
    }

    // Add subtransaction data with overflow handling
    if (snapshot->suboverflowed ||
        snapshot->subxcnt + numChildren > GetMaxSnapshotSubxidCount()) {
        appendStringInfoString(&buffer, "sof:1\\n");
    } else {
        appendStringInfoString(&buffer, "sof:0\\n");
        appendStringInfo(&buffer, "sxcnt:%d\\n", snapshot->subxcnt + numChildren);

        for (int i = 0; i < snapshot->subxcnt; i++) {
            appendStringInfo(&buffer, "sxp:%u\\n", snapshot->subxip[i]);
        }
        for (int i = 0; i < numChildren; i++) {
            appendStringInfo(&buffer, "sxp:%u\\n", children[i]);
        }
    }

    appendStringInfo(&buffer, "rec:%u\\n", snapshot->takenDuringRecovery);

    // Write to temporary file, then atomically rename
    snprintf(tempPath, sizeof(tempPath), "%s.tmp", filePath);
    file = AllocateFile(tempPath, PG_BINARY_W);

    fwrite(buffer.data, buffer.len, 1, file);
    FreeFile(file);
    rename(tempPath, filePath);

    // Return filename token for importing
    return pstrdup(filePath + strlen(SNAPSHOT_EXPORT_DIR) + 1);
}
```

Key simplifications made:
- Removed detailed error handling blocks and consolidated into basic checks
- Simplified variable naming for clarity (e.g., `nchildren` → `numChildren`)
- Reduced complex memory context switching to essential operations
- Consolidated file I/O operations and removed detailed error reporting
- Streamlined conditional logic for transaction ID handling
- Abstracted detailed buffer operations while preserving the serialization format
- Removed extensive comments and focused on core algorithm flow