# FreeSnapshotBuilder

## Location
[src/backend/replication/logical/snapbuild.c:372-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L372-L390)

## Overview
FreeSnapshotBuilder deallocates a snapshot builder and all its associated resources, properly cleaning up the memory context and releasing any held snapshots.

## Definition
```c
void FreeSnapshotBuilder(SnapBuild *builder)
```

## Detailed Description
This function performs the cleanup and deallocation of a SnapBuild structure. It first handles the explicit cleanup of any snapshot that may be held by the builder by decrementing its reference count through SnapBuildSnapDecRefcount. After ensuring the snapshot is properly released, it deallocates all remaining resources by deleting the entire memory context that was created during AllocateSnapshotBuilder. This approach ensures complete cleanup of all memory allocations associated with the snapshot builder.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildSnapDecRefcount](../S/SnapBuildSnapDecRefcount.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [FreeDecodingContext](FreeDecodingContext.md)

## Notes and Other Information
The function uses a two-step cleanup approach: first explicitly freeing the snapshot with reference counting (which includes error checking), then using memory context deletion to clean up all other resources. This design ensures that snapshots are properly managed with reference counting while other allocations are efficiently cleaned up through the memory context mechanism. The explicit snapshot cleanup is important because snapshots may be shared between different contexts and require proper reference counting.