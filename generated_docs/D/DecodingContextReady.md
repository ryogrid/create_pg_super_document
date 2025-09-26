# DecodingContextReady

## Location
[src/backend/replication/logical/logical.c:643-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L643-L651)

## Overview
DecodingContextReady determines whether a logical decoding context has built a consistent initial snapshot suitable for decoding operations.

## Definition

```c
bool
DecodingContextReady(LogicalDecodingContext *ctx)
```
## Detailed Description
This function provides a simple but critical check to determine if a logical decoding context is ready to begin processing WAL records for logical replication. It examines the snapshot builder's current state to verify that a consistent snapshot has been established.

The function returns true only when the snapshot builder has reached the SNAPBUILD_CONSISTENT state, which indicates that the snapshot can provide a consistent view of the database that is safe for logical decoding operations. This is essential because logical decoding requires a baseline snapshot that reflects a consistent point in time from which to start building logical changes.

The snapshot building process progresses through several states before reaching consistency, and this function provides a clean interface for callers to determine when the context is fully prepared for decoding work.

## Parameters / Member Variables
- : Pointer to the LogicalDecodingContext to check for readiness

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md): Returns the current state of the snapshot builder
  - SNAPBUILD_CONSISTENT: Constant representing the consistent snapshot state
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md): Structure containing the snapshot builder

- Called from (representative examples):
  - [DecodingContextFindStartpoint](DecodingContextFindStartpoint.md): During startpoint location determination
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md): When advancing slot position and checking snapshot state

## Notes and Other Information
- Simple boolean function with straightforward semantics
- Critical for ensuring logical decoding operations begin from a consistent database state
- The SNAPBUILD_CONSISTENT state indicates that all necessary transaction information has been gathered
- Used as a prerequisite check before beginning actual logical change processing
- Part of the snapshot building state machine that ensures transactional consistency in logical replication