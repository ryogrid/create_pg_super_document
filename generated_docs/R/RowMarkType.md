# RowMarkType

## Location
src/include/nodes/plannodes.h: 1335 - 1336

## Overview
An enumeration that defines different types of row-marking operations used for tuple locking and row identification in PostgreSQL's query execution, particularly for SELECT FOR UPDATE/SHARE operations.

## Definition


## Detailed Description
RowMarkType defines the various strategies PostgreSQL uses to mark and potentially lock rows during query execution. The first four values represent different lock strengths for SELECT FOR [KEY] UPDATE/SHARE requests, arranged in order of decreasing lock strength. These locking modes are supported on regular tables and foreign tables whose FDWs support late locking.

For non-lockable relations or when unique row identification is needed (during UPDATE/DELETE/MERGE operations), PostgreSQL uses either ROW_MARK_REFERENCE (fetch TID only) for regular tables or ROW_MARK_COPY (copy entire row) for cases where TID-based identification isn't possible (like VALUES or FUNCTION scans).

## Parameters / Member Variables
- : Strongest lock, equivalent to SELECT FOR UPDATE, prevents other transactions from reading or modifying the row
- : Lock for SELECT FOR NO KEY UPDATE, allows concurrent key-preserving updates
- : Shared lock for SELECT FOR SHARE, allows concurrent reads but prevents modifications
- : Weakest lock for SELECT FOR KEY SHARE, allows concurrent non-key modifications
- : No locking, just fetches the tuple identifier (TID) for later re-identification
- : Physically copies the entire row value for identification when TID is not available

## Dependencies
- Functions called/Symbols referenced:
  - RowMarkRequiresRowShareLock() macro
- Used by:
  - PlanRowMark struct (as markType member)
  - [ExecRowMark](../E/ExecRowMark.md) struct (as markType member)
  - Various planner and executor functions
  - Foreign Data Wrapper (FDW) interfaces

## Notes and Other Information
- The first four lock types (EXCLUSIVE through KEYSHARE) require actual tuple locking
- ROW_MARK_REFERENCE is more efficient but only works with relations that support TID-based access
- ROW_MARK_COPY is less efficient but works with any row source, used as fallback for complex cases
- Lock strength decreases from EXCLUSIVE to KEYSHARE, affecting concurrency and blocking behavior
- The RowMarkRequiresRowShareLock() macro tests if a mark type requires actual locking
- Foreign tables may use different strategies depending on FDW capabilities and remote locking support