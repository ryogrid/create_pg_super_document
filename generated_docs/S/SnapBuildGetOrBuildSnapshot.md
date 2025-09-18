# SnapBuildGetOrBuildSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:718-738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L718-L738)

## Overview
Ensures a snapshot exists for the current transaction by either returning an existing cached snapshot or building a new one if none exists.

## Definition


## Detailed Description
This function provides lazy initialization of snapshots for logical decoding operations. It implements a caching mechanism to avoid repeatedly building expensive snapshots when they haven't changed. The function:

1. **State Validation**: Ensures the builder is in SNAPBUILD_CONSISTENT state, meaning logical decoding has reached a point where consistent snapshots can be created.

2. **Cache Check**: Examines whether a snapshot is already cached in the builder structure.

3. **Lazy Building**: If no cached snapshot exists, calls SnapBuildBuildSnapshot to create a new one and stores it in the builder for future use.

4. **Reference Management**: Increments the reference count on newly built snapshots to ensure proper memory management and prevent premature cleanup.

This pattern allows logical decoding to efficiently reuse snapshots across multiple operations within the same decoding session, improving performance by avoiding redundant snapshot construction.

## Parameters / Member Variables
- : The SnapBuild structure that may contain a cached snapshot or from which to build a new snapshot

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildBuildSnapshot](SnapBuildBuildSnapshot.md)
  - [SnapBuildSnapIncRefcount](SnapBuildSnapIncRefcount.md)
  - SNAPBUILD_CONSISTENT
- Called from (representative examples):
  - [logicalmsg_decode](../l/logicalmsg_decode.md)

## Notes and Other Information
- Requires the builder to be in SNAPBUILD_CONSISTENT state
- Implements lazy evaluation - snapshots are only built when needed
- Uses reference counting to manage snapshot lifetime
- The cached snapshot persists across multiple calls until the builder state changes
- This function is typically used during the actual logical decoding process rather than during initial setup