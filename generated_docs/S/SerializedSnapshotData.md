# SerializedSnapshotData

## Location
src/backend/utils/time/snapmgr.c: 193 - 204

## Overview
A structure containing the essential fields from a PostgreSQL Snapshot that need to be serialized for transmission to cooperating backend processes.

## Definition

```c
typedef struct SerializedSnapshotData
{
	TransactionId xmin;
	TransactionId xmax;
	uint32		xcnt;
	int32		subxcnt;
	bool		suboverflowed;
	bool		takenDuringRecovery;
	CommandId	curcid;
	TimestampTz whenTaken;
	XLogRecPtr	lsn;
} SerializedSnapshotData;
```
## Detailed Description
SerializedSnapshotData represents a compact, serializable version of PostgreSQL's Snapshot structure. It contains only the essential fields that must be transmitted when sharing snapshots between backend processes, particularly during parallel query execution or distributed transaction processing.

This structure is designed for efficient serialization and deserialization of snapshot data. When a snapshot needs to be shared with a cooperating backend (such as in parallel workers), only these core fields are transmitted, while other fields like reference counts and internal pointers are reconstructed by the receiving process.

The structure serves as an intermediate representation between the full in-memory Snapshot structure and its serialized form in shared memory or inter-process communication channels.

## Parameters / Member Variables
- : Oldest transaction ID that was still active when the snapshot was taken
- : First transaction ID that was not yet assigned when the snapshot was taken  
- : Number of transaction IDs in the active XID array (xip)
- : Number of subtransaction IDs in the subXID array (subxip)
- : True if the subtransaction array overflowed and contains incomplete data
- : True if the snapshot was taken during WAL recovery
- : Current command ID within the transaction when snapshot was taken
- : Timestamp when the snapshot was created
- : WAL Log Sequence Number at the time the snapshot was taken

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (transaction identifier type)
  - CommandId (command identifier type)  
  - TimestampTz (timestamp with timezone type)
  - XLogRecPtr (WAL log position type)
  
- Called from (representative examples):
  - EstimateSnapshotSpace (calculates serialization space requirements)
  - SerializeSnapshot (creates serialized representation)
  - RestoreSnapshot (reconstructs snapshot from serialized data)

## Notes and Other Information
- Only contains fields that need serialization - receiving backend reconstructs remaining fields
- Used primarily for parallel query execution and worker process communication
- The XID and SubXID arrays (xip, subxip) are serialized separately after this structure
- SubXID array may be omitted during serialization if suboverflowed is true (except during recovery)
- Memory layout is designed for efficient memcpy() operations during serialization/deserialization
- Structure size is calculated by EstimateSnapshotSpace() to allocate appropriate shared memory
- Critical for maintaining MVCC consistency across parallel worker processes