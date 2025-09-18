# ReorderBufferCanStartStreaming

## Location
src/backend/replication/logical/reorderbuffer.c: 4159 - 4184

## Overview
Determines whether streaming of in-progress transactions can begin now, considering both plugin support and snapshot consistency requirements.

## Definition
```c
static inline bool ReorderBufferCanStartStreaming(ReorderBuffer *rb)
```

## Detailed Description
This function implements the logic to determine when streaming of large transactions can safely begin. It enforces two critical requirements for starting the streaming process:

1. **Snapshot Consistency**: The snapshot builder must have reached a consistent state (SNAPBUILD_CONSISTENT or higher) before streaming can begin. This ensures that the logical decoding process has a stable view of the database state.

2. **Transaction Restart Detection**: The function checks whether the current transaction was previously decoded and is being restarted from a checkpoint. If so, streaming is prevented to avoid duplicate or inconsistent change delivery.

The function combines basic streaming capability (checked via ReorderBufferCanStream) with these additional runtime conditions to provide a comprehensive streaming readiness check. This is essential for maintaining data consistency and avoiding conflicts during logical replication.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance containing the logical decoding context and snapshot builder

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferCanStream](ReorderBufferCanStream.md) (checks basic streaming support)
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md) (gets current snapshot builder state)
  - [SnapBuildXactNeedsSkip](../S/SnapBuildXactNeedsSkip.md) (checks if transaction should be skipped due to restart)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (accessed via rb->private_data)
  - [SnapBuild](../S/SnapBuild.md) (snapshot builder instance)
- Called from (representative examples):
  - [ReorderBufferCheckMemoryLimit](ReorderBufferCheckMemoryLimit.md) (before spilling vs streaming decision)
  - [ReorderBufferProcessPartialChange](ReorderBufferProcessPartialChange.md) (partial change streaming logic)
  - IsInsertOrUpdate (change processing decisions)

## Notes and Other Information
- Declared as static inline for performance optimization
- Requires both streaming support AND proper snapshot state
- Prevents streaming restarts to avoid duplicate data delivery
- Used in memory management decisions (stream vs spill to disk)
- Critical for maintaining logical replication consistency
- The SNAPBUILD_CONSISTENT state requirement ensures catalog visibility is stable