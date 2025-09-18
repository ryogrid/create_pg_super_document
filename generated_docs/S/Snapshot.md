# Snapshot

## Location
src/include/utils/snapshot.h: 121 - 122

## Overview
Snapshot is a typedef that represents a pointer to a SnapshotData structure, providing the primary interface for working with transaction snapshots in PostgreSQL.

## Definition
```c
typedef struct SnapshotData *Snapshot;
```

## Detailed Description
Snapshot serves as the standard handle for passing snapshot information throughout PostgreSQL's codebase. As a pointer type to SnapshotData, it provides an efficient way to reference snapshot structures without copying the entire structure. The typedef abstracts the implementation details and provides a clean interface for snapshot operations. PostgreSQL also defines InvalidSnapshot as ((Snapshot) NULL) for representing invalid or uninitialized snapshots.

## Parameters / Member Variables
- This is a pointer type with no direct members
- Points to a SnapshotData structure containing all snapshot information
- Can be InvalidSnapshot (NULL) to represent an invalid snapshot

## Dependencies
- Functions called/Symbols referenced:
  - SnapshotData (the underlying structure)
- Called from (representative examples):
  - Snapshot management functions in snapmgr.c
  - Heap access methods
  - Index access methods
  - Various visibility checking functions

## Notes and Other Information
The Snapshot typedef provides type safety and code clarity when working with snapshots. It's the primary type used throughout PostgreSQL for passing snapshot information between functions. The pointer-based approach allows for efficient parameter passing and enables snapshot sharing between different operations. InvalidSnapshot serves as a sentinel value similar to NULL pointer patterns in other PostgreSQL subsystems.