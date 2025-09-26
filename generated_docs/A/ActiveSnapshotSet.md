# ActiveSnapshotSet

## Location
src/backend/utils/time/snapmgr.c: 782 - 793

## Overview
Checks whether there is at least one snapshot in the active snapshot stack.

## Definition

```c
bool
ActiveSnapshotSet(void)
```
## Detailed Description
ActiveSnapshotSet is a simple utility function that determines whether an active snapshot is currently available. It returns true if there is at least one snapshot in the active snapshot stack, and false if the stack is empty. This function is commonly used as a guard condition before calling other snapshot-related functions that require an active snapshot to be present, helping to prevent errors and ensure proper snapshot management throughout the system.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - None (directly checks global ActiveSnapshot variable)
- Called from (representative examples):
  - index_concurrently_build
  - find_inheritance_children_extended
  - ReindexMultipleInternal
  - vacuum
  - postquel_start
  - _SPI_execute_plan
  - RelationGetPartitionDesc
  - pg_plan_query
  - PortalRunUtility
  - EnsurePortalSnapshotExists
  - RevalidateCachedQuery

## Notes and Other Information
- Simple boolean check function for active snapshot existence
- Commonly used as a guard condition before snapshot operations
- Essential for defensive programming in snapshot-dependent code paths
- Helps prevent null pointer access to ActiveSnapshot
- Used across various PostgreSQL subsystems (executor, planner, utilities)
- Returns false when no snapshots are active, true otherwise