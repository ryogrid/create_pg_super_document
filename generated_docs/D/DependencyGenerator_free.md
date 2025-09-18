# DependencyGenerator_free

## Location
src/backend/statistics/dependencies.c: 196 - 203

## Overview
Deallocates memory used by a DependencyGenerator, freeing both the dependencies array and the generator state structure.

## Definition
```c
static void DependencyGenerator_free(DependencyGenerator state)
```

## Detailed Description
This destructor function properly cleans up all memory allocated by a DependencyGenerator. It performs a two-step deallocation process:
1. First frees the dependencies array that holds all the pre-generated functional dependencies
2. Then frees the DependencyGeneratorData structure itself

The function follows PostgreSQL's memory management conventions using pfree() for both deallocations. This ensures proper cleanup and prevents memory leaks when the generator is no longer needed.

## Parameters / Member Variables
- `state`: The DependencyGenerator to be freed, containing both the generator state and the dependencies array

## Dependencies
- Functions called/Symbols referenced:
  - pfree (memory deallocation, called twice)
- Called from (representative examples):
  - statext_dependencies_build

## Notes and Other Information
- This is a static function internal to the dependencies.c module
- Must be called to avoid memory leaks after using a DependencyGenerator
- The function assumes the state parameter is not NULL and was properly initialized
- Should be the final operation performed on a DependencyGenerator instance
- The order of freeing is important: dependencies array first, then the state structure
- Located at src/backend/statistics/dependencies.c:196-203