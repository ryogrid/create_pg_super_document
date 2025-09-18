# ParallelTableScanDesc

## Location
src/include/access/relscan.h: 70 - 74

## Overview
ParallelTableScanDesc is a pointer type definition to ParallelTableScanDescData, providing a handle for accessing shared parallel table scan state.

## Definition
```c
typedef struct ParallelTableScanDescData *ParallelTableScanDesc;
```

## Detailed Description
ParallelTableScanDesc is a typedef that creates a pointer type to the ParallelTableScanDescData structure. This provides a standard interface for accessing and manipulating the shared state information required for parallel table scans. The typedef abstracts the underlying structure and provides a consistent API for parallel scan coordination throughout the PostgreSQL codebase. This handle is typically stored within individual TableScanDesc structures to link private backend scan state with shared parallel coordination data.

## Parameters / Member Variables
This is a pointer typedef, so it does not have direct member variables. It points to a ParallelTableScanDescData structure which contains the shared parallel scan state and coordination information.

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelTableScanDescData](ParallelTableScanDescData.md)
- Called from (representative examples):
  - Parallel table scan initialization functions
  - Table access method parallel scan implementations
  - [Query](../Q/Query.md) execution nodes that support parallel scanning

## Notes and Other Information
This typedef is defined in src/include/access/relscan.h (line 70). It serves as the standard handle type for parallel table scan coordination and is used throughout PostgreSQL's parallel execution infrastructure. The pointer-based approach allows multiple backend processes to reference the same shared memory structure while maintaining their own private scan state. This design enables efficient parallel table scanning by allowing workers to coordinate their efforts while minimizing synchronization overhead.