# SnapBuildExportSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:678-717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L678-L717)

## Overview
Exports a logical decoding snapshot so it can be imported and used in another session via SET TRANSACTION SNAPSHOT, enabling consistent reads across different database connections.

## Definition

```c
const char *
SnapBuildExportSnapshot(SnapBuild *builder)
```
## Detailed Description
This function creates an exportable snapshot from a logical decoding builder state. The exported snapshot can be used by other sessions to establish the same consistent view of the database. The function performs several key operations:

1. **Transaction Management**: Starts a new transaction with REPEATABLE READ isolation and read-only mode to ensure the snapshot remains valid during export.

2. **State Validation**: Ensures no transaction is currently active and prevents multiple concurrent exports by checking SavedResourceOwnerDuringExport.

3. **Snapshot Creation**: Uses SnapBuildInitialSnapshot to build the actual snapshot from the builder state.

4. **Export Process**: Utilizes the standard PostgreSQL ExportSnapshot mechanism to make the snapshot available for other sessions.

5. **Resource Tracking**: Manages ExportInProgress flag and SavedResourceOwnerDuringExport to coordinate the export lifecycle.

The function ensures that the source transaction remains open so that importing sessions can verify the xmin horizon hasn't advanced, maintaining consistency guarantees.

## Parameters / Member Variables
- `*builder`: The SnapBuild structure containing the logical decoding state from which to create the exportable snapshot
## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md)
  - [StartTransactionCommand](StartTransactionCommand.md)
  - [SnapBuildInitialSnapshot](SnapBuildInitialSnapshot.md)
  - [ExportSnapshot](../E/ExportSnapshot.md)
  - [errmsg_plural](../e/errmsg_plural.md)
- Called from (representative examples):
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
- Must be called outside of any existing transaction or transaction block
- Only allows one snapshot export at a time per backend
- The exported snapshot uses REPEATABLE READ isolation level and is read-only
- Returns the snapshot name that can be used with SET TRANSACTION SNAPSHOT
- Logs the export event with transaction count information
- The transaction started by this function must remain open until the snapshot is no longer needed by importing sessions

## Simplified Source

```c
// Simplified version of SnapBuildExportSnapshot
const char *SnapBuildExportSnapshot(SnapBuild *builder) {
    Snapshot snap;
    char *snapname;

    // Ensure we're not in a transaction
    if (IsTransactionOrTransactionBlock()) {
        elog(ERROR, "cannot export a snapshot from within a transaction");
    }

    // Prevent concurrent exports
    if (SavedResourceOwnerDuringExport) {
        elog(ERROR, "can only export one snapshot at a time");
    }

    // Set up export state
    SavedResourceOwnerDuringExport = CurrentResourceOwner;
    ExportInProgress = true;

    // Start a transaction for the export
    StartTransactionCommand();

    // Configure transaction properties
    XactIsoLevel = XACT_REPEATABLE_READ;
    XactReadOnly = true;

    // Build the initial snapshot
    snap = SnapBuildInitialSnapshot(builder);

    // Export the snapshot using standard mechanism
    snapname = ExportSnapshot(snap);

    // Log the export event
    ereport(LOG, (errmsg_plural(
        "exported logical decoding snapshot: \"%s\" with %u transaction ID",
        "exported logical decoding snapshot: \"%s\" with %u transaction IDs",
        snap->xcnt, snapname, snap->xcnt)));

    return snapname;
}
```

Key simplifications made:
- Added clear comments for each validation and setup step
- Maintained all essential error checking
- Preserved transaction state management
- Kept the logging for operational visibility