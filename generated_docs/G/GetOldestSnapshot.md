# GetOldestSnapshot

## Location
[src/backend/utils/time/snapmgr.c:323-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L323-L351)

## Overview
Retrieves the transactions oldest known snapshot, determined by comparing LSN (Log Sequence Number) values between active and registered snapshots.

## Definition
```c
Snapshot GetOldestSnapshot(void)
```

## Detailed Description
GetOldestSnapshot identifies and returns the oldest snapshot currently maintained by the transaction, using LSN values to determine chronological ordering. The function examines both registered snapshots (stored in a pairing heap) and active snapshots to find the one with the smallest LSN, indicating it represents the earliest point in the transaction log. This is useful for operations that need to work with the oldest available view of the database, such as TOAST (The Oversized-Attribute Storage Technique) operations that must maintain consistency with older snapshot states.

The function prioritizes active snapshots over registered snapshots when they have older or equivalent LSN values, and returns NULL if no snapshots are currently active or registered.

## Parameters / Member Variables
- Returns: The oldest Snapshot based on LSN comparison, or NULL if no snapshots exist

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - pairingheap_container
  - [SnapshotData](../S/SnapshotData.md)
  - [pairingheap_first](../p/pairingheap_first.md)
  - XLogRecPtrIsInvalid
- Called from (representative examples):
  - [init_toast_snapshot](../i/init_toast_snapshot.md)
  - IsMVCCSnapshot

## Notes and Other Information
- Uses LSN (Log Sequence Number) for chronological comparison of snapshots
- Examines both registered snapshots (in pairing heap) and active snapshots
- Returns NULL when no active or registered snapshots exist
- Prioritizes active snapshots when LSNs are equal or active snapshot is older
- Essential for TOAST operations that require consistency with older snapshot states
- The LSN-based ordering ensures proper chronological snapshot selection
- Part of PostgreSQLs snapshot management system for maintaining MVCC consistency