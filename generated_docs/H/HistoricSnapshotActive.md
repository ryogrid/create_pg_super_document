# HistoricSnapshotActive

## Location
[src/backend/utils/time/snapmgr.c:1672-1677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1672-L1677)

## Overview
HistoricSnapshotActive is a utility function that checks whether a historical snapshot is currently active for catalog access during logical decoding operations.

## Definition
```c
bool HistoricSnapshotActive(void)
```

## Detailed Description
This simple but important function provides a way to determine if the system is currently operating in historical snapshot mode. It serves as a guard condition that other parts of the PostgreSQL system can use to modify their behavior when logical decoding is active.

The function simply checks if the global HistoricSnapshot variable is non-NULL, which indicates that SetupHistoricSnapshot has been called and historical catalog access is currently enabled. This allows various subsystems to take appropriate action when they need to behave differently during logical decoding operations.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - HistoricSnapshot (global variable check)
  - HTAB (referenced in context)
- Called from (representative examples):
  - SnapBuildInitialSnapshot (in logical snapbuild)
  - RelationInitPhysicalAddr (in relation cache)
  - RelationClearRelation (in relation cache)
  - RelationGetIdentityKeyBitmap (in relation cache)
  - GetTransactionSnapshot (in snapshot manager)
  - GetLatestSnapshot (in snapshot manager)
  - GetCatalogSnapshot (in snapshot manager)
  - SetTransactionSnapshot (in snapshot manager)
  - HistoricSnapshotGetTupleCids (in same file)

## Notes and Other Information
- This is a simple predicate function that returns true if historical snapshot mode is active
- Used extensively throughout the PostgreSQL codebase to conditionally modify behavior during logical decoding
- The function enables different subsystems to detect when they're operating in logical decoding context
- Located in src/backend/utils/time/snapmgr.c at lines 1672-1677
- Essential for maintaining consistency between normal operations and logical decoding operations
- The return value directly corresponds to whether SetupHistoricSnapshot has been called without a corresponding TeardownHistoricSnapshot