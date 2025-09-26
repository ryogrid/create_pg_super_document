# ExportedSnapshot

## Location
[src/backend/utils/time/snapmgr.c:148-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L148-L152)

## Overview
A structure that holds information about PostgreSQL snapshots that have been exported to files for sharing between backend processes.

## Definition

```c
typedef struct ExportedSnapshot
{
	char	   *snapfile;
	Snapshot	snapshot;
} ExportedSnapshot;
```
## Detailed Description
ExportedSnapshot is a container structure used in PostgreSQL's snapshot export/import mechanism. It represents a snapshot that has been serialized to a file and can be shared with other backend processes. This functionality is crucial for maintaining consistent read views across different database connections, particularly useful for logical replication, parallel processing, and distributed transactions.

When a snapshot is exported using the ExportSnapshot() function, an ExportedSnapshot structure is created to track both the file path where the snapshot data is stored and a reference to the actual snapshot object. The exported snapshots are maintained in a list (exportedSnapshots) for the duration of the transaction to ensure the snapshot's xmin value is honored and the snapshot remains valid.

The structure is allocated in TopTransactionContext to ensure it persists for the entire transaction lifetime, and the associated snapshot is pseudo-registered to prevent premature cleanup.

## Parameters / Member Variables
- : Path to the file where the snapshot has been exported (stored as a string)
- : Reference to the actual Snapshot structure containing the snapshot data

## Dependencies
- Functions called/Symbols referenced:
  - Snapshot (data type)
  - char (basic data type)
  
- Called from (representative examples):
  - ExportSnapshot (creates and manages ExportedSnapshot instances)
  - AtEOXact_Snapshot (cleanup of exported snapshots at transaction end)

## Notes and Other Information
- ExportedSnapshot structures are stored in the exportedSnapshots global list
- Memory allocation occurs in TopTransactionContext for transaction-lifetime persistence  
- The snapfile path follows the format: SNAPSHOT_EXPORT_DIR/VXID-COUNTER where VXID is the virtual transaction ID
- Associated snapshots are pseudo-registered (regd_count incremented) to prevent premature cleanup
- Used primarily for pg_export_snapshot() SQL function functionality
- Critical for maintaining MVCC consistency across multiple database connections
- Snapshot files contain serialized transaction state information including xmin, xmax, active XIDs, and isolation level