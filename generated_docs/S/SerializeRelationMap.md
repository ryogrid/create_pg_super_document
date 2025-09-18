# SerializeRelationMap

## Location
[src/backend/utils/cache/relmapper.c:724-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L724-L740)

## Overview
SerializeRelationMap serializes the current active shared and local relation map states into a memory buffer for transmission to parallel worker processes.

## Definition
```c
void SerializeRelationMap(Size maxSize, char *startAddress)
```

## Detailed Description
This function packages the active relation map updates (both shared and local) into a SerializedActiveRelMaps structure at the specified memory address. It copies the current active_shared_updates and active_local_updates into the serialized format, enabling parallel workers to receive and apply the same relation mapping state as the leader process. The function includes an assertion to ensure the provided buffer is large enough, as determined by EstimateRelationMapSpace(). This serialization is essential for maintaining consistency between the leader and worker processes in parallel query execution.

## Parameters / Member Variables
- `maxSize`: Maximum size of the buffer available for serialization (in bytes)
- `startAddress`: Pointer to the memory buffer where serialized data will be written

## Dependencies
- Functions called/Symbols referenced:
  - [SerializedActiveRelMaps](SerializedActiveRelMaps.md) (type reference)
  - [EstimateRelationMapSpace](../E/EstimateRelationMapSpace.md)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (at src/backend/access/transam/parallel.c:434)

## Notes and Other Information
- Asserts that maxSize is at least as large as EstimateRelationMapSpace() returns
- Serializes only the active updates, not the entire relation maps
- Part of the parallel query infrastructure for maintaining mapping consistency
- Works in conjunction with EstimateRelationMapSpace for proper memory management
- Critical for ensuring parallel workers can access system catalogs with the same OID-to-filenode mappings as the leader process
- The serialized data must be restored in worker processes using a corresponding deserialization function