# SnapBuild

## Location
src/backend/replication/logical/snapbuild.c: 152 - 323

## Overview
SnapBuild is a core data structure in PostgreSQL's logical replication system that manages the state and process of building consistent snapshots for logical decoding, tracking transaction visibility and catalog changes during WAL replay.

## Definition


## Detailed Description
SnapBuild manages the complex process of building consistent snapshots for logical replication in PostgreSQL. It tracks the progression through different states (START → BUILDING_SNAPSHOT → FULL_SNAPSHOT → CONSISTENT) while maintaining transaction visibility information and catalog change tracking.

The structure coordinates with the ReorderBuffer to provide consistent views of the database catalog during logical decoding, ensuring that decoded changes reflect a coherent state of the database. It handles both full snapshots for complete logical replication and catalog-only snapshots for more efficient operations.

Key responsibilities include:
- Tracking transaction visibility boundaries (xmin/xmax)
- Managing catalog change detection and recording
- Coordinating snapshot serialization and restoration
- Supporting two-phase commit protocols
- Maintaining consistency during slot creation

## Parameters / Member Variables
- : Current phase of snapshot building (SnapBuildState enum)
- : Private memory context for all allocations in this module
- : Lower bound - all transactions below this have committed/aborted
- : Upper bound - all transactions at or above this are uncommitted
- : LSN threshold below which commits should not be replayed
- : LSN where two-phase decoding was enabled or consistency found
- : Minimum xid threshold for starting WAL decoding
- : Flag indicating full vs catalog-only snapshot building
- : Flag indicating snapshot builder is for slot creation
- : Current valid snapshot for seeing catalog state
- : LSN of last confirmed snapshot serialization
- : Associated ReorderBuffer for snapshot coordination
- : Transaction ID triggering next snapshot building phase
- : Count of committed transactions with potential catalog changes
- : Allocated space in the committed transactions array
- : Whether all transactions are recorded (before CONSISTENT state)
- : Unsorted array of committed transaction IDs with catalog changes
- : Count of catalog-changing transactions running during serialization
- : Sorted array of transaction IDs that modified catalogs

## Dependencies
- Functions called/Symbols referenced:
  - SnapBuildState (enum for tracking build phases)
  - [ReorderBuffer](../R/ReorderBuffer.md) (coordination with transaction reordering)
  - [MemoryContext](../M/MemoryContext.md) (memory management)
  - [Snapshot](Snapshot.md) (PostgreSQL snapshot structure)
  - TransactionId, XLogRecPtr (core PostgreSQL types)

- Called from (representative examples):
  - [AllocateSnapshotBuilder](../A/AllocateSnapshotBuilder.md) (creates and initializes SnapBuild)
  - SnapBuildProcessRunningXacts (processes running transaction records)
  - SnapBuildCommitTxn (handles transaction commit processing)
  - [SnapBuildSerialize](SnapBuildSerialize.md)/SnapBuildRestore (snapshot persistence)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (main logical decoding coordination)

## Notes and Other Information
- This structure is private to snapbuild.c and not exposed in public headers
- The committed.xip array is intentionally kept unsorted for performance during frequent modifications, only sorted when building snapshots
- The catchange array stores transactions that were running during serialization to handle cases where only commit records are decoded after snapshot restoration
- Two-phase commit support requires careful coordination with the two_phase_at LSN
- Memory management is handled through the dedicated context to ensure proper cleanup