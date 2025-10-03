# RestoreRelationMap

## Location
[src/backend/utils/cache/relmapper.c:741-764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L741-L764)

## Overview
RestoreRelationMap restores the active shared and local relation mapping state within a parallel worker process from serialized data.

## Definition

```c
void
RestoreRelationMap(char *startAddress)
```
## Detailed Description
RestoreRelationMap is used to restore relation mapping state in parallel worker processes. The function takes a serialized representation of active relation mappings and restores them into the parallel worker's local state. This is essential for parallel query execution where workers need access to the same relation mapping information as the main process.

The function performs validation to ensure that the parallel worker doesn't have any existing mappings before restoration, throwing an error if any active or pending mappings are found. This prevents conflicts and ensures clean state initialization.

## Parameters / Member Variables
- `*startAddress`: Pointer to the serialized relation mapping data (SerializedActiveRelMaps structure)
## Dependencies
- Functions called/Symbols referenced:
  - [SerializedActiveRelMaps](../S/SerializedActiveRelMaps.md) (structure type)
  - elog (error logging)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (at src/backend/access/transam/parallel.c:1517)

## Notes and Other Information
- This function is specifically designed for parallel query execution support
- It validates that the worker process has a clean state before restoration
- Only restores active mappings, not pending ones
- Part of PostgreSQL's relation mapper subsystem which handles mapping between relation OIDs and physical file locations
- The function assumes the serialized data format matches SerializedActiveRelMaps structure