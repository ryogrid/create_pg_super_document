# PushActiveSnapshotWithLevel

## Location
src/backend/utils/time/snapmgr.c: 662 - 699

## Overview
Sets the given snapshot as the current active snapshot with a specified transaction nesting level, managing snapshot copying and the active snapshot stack.

## Definition
```c
void PushActiveSnapshotWithLevel(Snapshot snapshot, int snap_level)
```

## Detailed Description
This function implements the core logic for pushing snapshots onto the active snapshot stack. It handles the complexity of determining when snapshots need to be copied versus when they can be used directly, and manages the stack of active snapshots with their associated transaction nesting levels.

Key behaviors:
- Creates a new ActiveSnapshotElt to track the snapshot and its nesting level
- Copies snapshots that are statically allocated (CurrentSnapshot, SecondarySnapshot) or not already copied
- Maintains proper stack ordering by nesting level (must be >= current top level)
- Increments active_count reference counter on the snapshot
- Updates OldestActiveSnapshot tracking when this becomes the first active snapshot

The function ensures snapshot lifetime management and proper isolation by maintaining a stack of active snapshots that can be popped when operations complete.

## Parameters / Member Variables
- `snapshot`: The snapshot to make active, must not be InvalidSnapshot
- `snap_level`: The transaction nesting level that owns this snapshot, must be >= current top level

## Dependencies
- Functions called/Symbols referenced:
  - ActiveSnapshotElt
  - InvalidSnapshot  
  - MemoryContextAlloc
  - CopySnapshot
- Called from (representative examples):
  - PortalRunUtility
  - EnsurePortalSnapshotExists
  - PushActiveSnapshot
  - IsMVCCSnapshot

## Notes and Other Information
- This is a public function exported via snapmgr.h
- More flexible than PushActiveSnapshot as it allows specifying the ownership level
- The snapshot copying logic prevents issues with statically allocated snapshots that might be modified
- Maintains both ActiveSnapshot (top of stack) and OldestActiveSnapshot (bottom of stack) pointers
- The as_level field ensures proper nesting - snapshots can only be pushed at levels >= current top
- Used internally by PushActiveSnapshot and directly by portal management code
- Should be balanced with PopActiveSnapshot calls to maintain stack integrity