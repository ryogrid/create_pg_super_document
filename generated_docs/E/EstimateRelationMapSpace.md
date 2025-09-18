# EstimateRelationMapSpace

## Location
[src/backend/utils/cache/relmapper.c:713-723](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L713-L723)

## Overview
EstimateRelationMapSpace calculates the memory space required to serialize and pass active relation maps to parallel worker processes.

## Definition
```c
Size EstimateRelationMapSpace(void)
```

## Detailed Description
This function provides a size estimate for the space needed to serialize the current active shared and local relation maps when setting up parallel query execution. It returns the size of the SerializedActiveRelMaps structure, which is used to package the relation mapping information for transmission to parallel workers. This estimation is crucial for proper memory allocation in the parallel query infrastructure, ensuring that worker processes receive the necessary relation mapping data to access system catalogs correctly.

## Parameters / Member Variables
This function takes no parameters and returns a Size value representing the required space in bytes.

## Dependencies
- Functions called/Symbols referenced:
  - [SerializedActiveRelMaps](../S/SerializedActiveRelMaps.md) (type reference)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (at src/backend/access/transam/parallel.c:286)
  - [SerializeRelationMap](../S/SerializeRelationMap.md) (at src/backend/utils/cache/relmapper.c:728)

## Notes and Other Information
- Returns a fixed size based on the SerializedActiveRelMaps structure
- Part of the parallel query infrastructure for sharing relation mapping state
- Used in conjunction with SerializeRelationMap to transfer mapping data to workers
- Critical for ensuring parallel workers can access system catalogs with correct OID-to-filenode mappings
- The estimate is conservative and covers both shared and local relation maps