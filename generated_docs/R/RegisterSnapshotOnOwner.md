# RegisterSnapshotOnOwner

## Location
src/backend/utils/time/snapmgr.c: 807 - 835

## Overview
Registers a snapshot as being in use by a specified resource owner, handling snapshot copying, reference counting, and pairing heap management.

## Definition
```c
Snapshot RegisterSnapshotOnOwner(Snapshot snapshot, ResourceOwner owner)
```

## Detailed Description
RegisterSnapshotOnOwner is the core function for snapshot registration in PostgreSQL's snapshot management system. It performs several critical operations: validates the input snapshot, creates a persistent copy if the snapshot is static (not already copied), increments the reference count, registers with the resource owner system, and adds the snapshot to the RegisteredSnapshots pairing heap if it's the first registration. This function ensures proper memory management and reference tracking for MVCC snapshots.

## Parameters / Member Variables
- `snapshot`: The snapshot to register. Must not be InvalidSnapshot for successful registration.
- `owner`: The ResourceOwner that will be responsible for tracking this snapshot registration.

## Dependencies
- Functions called/Symbols referenced:
  - CopySnapshot
  - ResourceOwnerEnlarge
  - ResourceOwnerRememberSnapshot
  - pairingheap_add
  - InvalidSnapshot
- Called from (representative examples):
  - RegisterSnapshot
  - be_lo_open

## Notes and Other Information
- Creates a persistent copy of static snapshots using CopySnapshot
- Increments the snapshot's regd_count reference counter
- Registers with ResourceOwner system for proper cleanup
- Adds snapshot to RegisteredSnapshots pairing heap on first registration
- Returns InvalidSnapshot unchanged if passed InvalidSnapshot
- Essential for MVCC consistency and proper snapshot lifecycle management
- Located in src/backend/utils/time/snapmgr.c:807-835