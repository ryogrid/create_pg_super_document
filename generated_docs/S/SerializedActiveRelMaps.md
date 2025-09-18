# SerializedActiveRelMaps

## Location
[src/backend/utils/cache/relmapper.c:101-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L101-L105)

## Overview
SerializedActiveRelMaps is a structure used to serialize the current state of relation mappings for parallel workers, containing both shared and local active relation map updates.

## Definition
```c
typedef struct SerializedActiveRelMaps
{
    RelMapFile  active_shared_updates;
    RelMapFile  active_local_updates;
} SerializedActiveRelMaps;
```

## Detailed Description
SerializedActiveRelMaps is specifically designed for PostgreSQL's parallel processing infrastructure. It encapsulates the active relation mapping state that needs to be communicated to parallel worker processes. The structure contains two RelMapFile instances representing the current active updates for both shared system catalogs and local (database-specific) catalogs.

This serialization mechanism is essential for maintaining consistency across parallel workers, ensuring that all processes have access to the same relation mapping information. The structure captures only the active state of relation mappings, excluding any pending updates that haven't yet been committed.

The separation of shared and local updates reflects PostgreSQL's two-tier catalog system, where some catalogs are shared across all databases in a cluster (like pg_database) while others are specific to individual databases (like pg_class within a particular database).

## Parameters / Member Variables
- `active_shared_updates`: RelMapFile containing the current active relation mappings for shared system catalogs that apply cluster-wide
- `active_local_updates`: RelMapFile containing the current active relation mappings for local system catalogs specific to the current database

## Dependencies
- Functions called/Symbols referenced:
  - [RelMapFile](../R/RelMapFile.md) (struct type for both member variables)
- Called from (representative examples):
  - [EstimateRelationMapSpace](../E/EstimateRelationMapSpace.md)
  - [SerializeRelationMap](SerializeRelationMap.md)
  - [RestoreRelationMap](../R/RestoreRelationMap.md)

## Notes and Other Information
- Used exclusively for parallel worker communication and synchronization
- Contains only active relation mapping states, not pending updates
- Essential for maintaining mapping consistency across parallel processes
- The structure size can be estimated using EstimateRelationMapSpace() for memory allocation purposes
- Supports both serialization (via SerializeRelationMap) and deserialization (via RestoreRelationMap) operations
- Part of PostgreSQL's broader parallel processing infrastructure that ensures all workers have consistent views of system catalog file locations