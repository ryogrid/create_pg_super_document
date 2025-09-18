# ExportSnapshot

## Location
src/backend/utils/time/snapmgr.c: 1095 - 1271

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
  - GetTopTransactionIdIfAny
  - IsSubTransaction  
  - xactGetCommittedChildren
  - TransactionIdPrecedes
  - CopySnapshot
  - GetMaxSnapshotSubxidCount
  - AllocateFile
  - FreeFile
  - pairingheap_add
- Called from (representative examples):
  - pg_export_snapshot
  - SnapBuildExportSnapshot

## Notes and Other Information
- Cannot be called from within a subtransaction due to verification complexities
- The function includes committed child subtransactions in the exported snapshot
- File creation uses atomic rename operations (.tmp suffix) to prevent partial reads
- The returned token is the basename of the created file, used for importing the snapshot
- Snapshots are stored in TopTransactionContext and registered to maintain xmin visibility
- No fsync is performed as the snapshot file does not need to survive system crashes