# SnapBuildCurrentState

## Location
[src/backend/replication/logical/snapbuild.c:416-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L416-L424)

## Overview
SnapBuildCurrentState returns the current state of the snapshot building process, providing a simple accessor to query the builder's progression through the snapshot construction phases.

## Definition
```c
SnapBuildState SnapBuildCurrentState(SnapBuild *builder)
```

## Detailed Description
This function serves as a simple getter method that returns the current state of a snapshot builder. The snapshot building process in logical replication goes through several distinct states as it progresses from initialization to producing consistent snapshots. This function allows other components of the logical replication system to query the current phase of snapshot construction to make appropriate decisions about transaction processing and decoding.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure whose state is being queried

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuild](SnapBuild.md) (structure access)
- Called from (representative examples):
  - [xact_decode](../x/xact_decode.md)
  - [heap2_decode](../h/heap2_decode.md)
  - [heap_decode](../h/heap_decode.md)
  - [logicalmsg_decode](../l/logicalmsg_decode.md)
  - [DecodePrepare](../D/DecodePrepare.md)
  - DecodingContextReady
  - [ReorderBufferCanStartStreaming](../R/ReorderBufferCanStartStreaming.md)

## Notes and Other Information
This is a simple accessor function that directly returns the state field from the SnapBuild structure. The state follows the SnapBuildState enumeration which typically includes states like SNAPBUILD_START, SNAPBUILD_FULL_SNAPSHOT, SNAPBUILD_CONSISTENT, etc. The function is widely used throughout the logical decoding system to determine whether the snapshot builder is ready to process different types of WAL records and whether consistent snapshots are available for transaction decoding.