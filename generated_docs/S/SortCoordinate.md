# SortCoordinate

## Location
src/include/utils/tuplesort.h: 61 - 76

## Overview
SortCoordinate is a pointer type to SortCoordinateData used for passing coordination state in parallel tuplesort operations.

## Definition
```c
typedef struct SortCoordinateData *SortCoordinate;
```

## Detailed Description
SortCoordinate is a typedef that creates a pointer type to SortCoordinateData structures. This abstraction provides a cleaner interface for functions that need to pass around coordination state for parallel sorting operations. By using a pointer type, it allows functions to easily pass the coordination structure by reference rather than by value, which is essential for maintaining shared state information across multiple function calls during parallel sort operations.

The use of this typedef follows PostgreSQL conventions of creating pointer typedefs for commonly-passed structure types, making function signatures more readable and reducing the need to explicitly use pointer syntax throughout the codebase.

## Parameters / Member Variables
This is a pointer type, so it points to the members of SortCoordinateData:
- Points to `isWorker`: Worker process identification flag
- Points to `nParticipants`: Number of participating processes
- Points to `sharedsort`: Shared memory state pointer

## Dependencies
- Functions called/Symbols referenced:
  - SortCoordinateData (the underlying structure)
- Called from (representative examples):
  - Various parallel sort coordination functions that need to pass coordination state

## Notes and Other Information
- This is simply a convenience typedef for SortCoordinateData pointer
- Commonly used in function parameters where coordination state needs to be passed by reference
- Enables cleaner function signatures in the parallel tuplesort API
- NULL SortCoordinate values typically indicate non-parallel sorting operations