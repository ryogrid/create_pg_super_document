# SnapBuildExportSnapshot

## Location
src/backend/replication/logical/snapbuild.c: 678 - 717

## Overview
Exports a logical decoding snapshot so it can be imported and used in another session via SET TRANSACTION SNAPSHOT, enabling consistent reads across different database connections.

## Definition


## Detailed Description
This function creates an exportable snapshot from a logical decoding builder state. The exported snapshot can be used by other sessions to establish the same consistent view of the database. The function performs several key operations:

1. **Transaction Management**: Starts a new transaction with REPEATABLE READ isolation and read-only mode to ensure the snapshot remains valid during export.

2. **State Validation**: Ensures no transaction is currently active and prevents multiple concurrent exports by checking SavedResourceOwnerDuringExport.

3. **Snapshot Creation**: Uses SnapBuildInitialSnapshot to build the actual snapshot from the builder state.

4. **Export Process**: Utilizes the standard PostgreSQL ExportSnapshot mechanism to make the snapshot available for other sessions.

5. **Resource Tracking**: Manages ExportInProgress flag and SavedResourceOwnerDuringExport to coordinate the export lifecycle.

The function ensures that the source transaction remains open so that importing sessions can verify the xmin horizon hasn't advanced, maintaining consistency guarantees.

## Parameters / Member Variables
- : The SnapBuild structure containing the logical decoding state from which to create the exportable snapshot

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionOrTransactionBlock
  - StartTransactionCommand
  - SnapBuildInitialSnapshot
  - ExportSnapshot
  - errmsg_plural
- Called from (representative examples):
  - CreateReplicationSlot

## Notes and Other Information
- Must be called outside of any existing transaction or transaction block
- Only allows one snapshot export at a time per backend
- The exported snapshot uses REPEATABLE READ isolation level and is read-only
- Returns the snapshot name that can be used with SET TRANSACTION SNAPSHOT
- Logs the export event with transaction count information
- The transaction started by this function must remain open until the snapshot is no longer needed by importing sessions