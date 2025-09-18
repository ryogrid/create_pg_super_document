# addObjectDependency

## Location
src/bin/pg_dump/common.c: 807 - 831

## Overview
Adds a dependency link to a DumpableObject, establishing that one object depends on another during the dump process.

## Definition


## Detailed Description
This function manages the dependency relationships between database objects in pg_dump by adding a reference ID to a DumpableObject's dependency list. The function implements dynamic memory management for the dependencies array, starting with an initial allocation of 16 DumpId slots and doubling the allocation size whenever more space is needed. This approach ensures efficient memory usage while accommodating objects with varying numbers of dependencies.

The dependency tracking is crucial for pg_dump's operation as it ensures objects are dumped in the correct order - dependencies must be dumped before the objects that depend on them. The function allows duplicate dependencies to be added, which may be intentional for certain dependency tracking scenarios.

## Parameters / Member Variables
- : Pointer to the DumpableObject that will have a new dependency added to its dependency list
- : The DumpId of the object that this DumpableObject depends on

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_array (initial memory allocation for dependencies array)
  - pg_realloc_array (memory reallocation when expanding dependencies array)  
  - DumpableObject (structure type for dumpable objects)
  - DumpId (type for dump object identifiers)
- Called from (representative examples):
  - [flagInhTables](../f/flagInhTables.md) (in src/bin/pg_dump/common.c:399-400)
  - [flagInhIndexes](../f/flagInhIndexes.md) (in src/bin/pg_dump/common.c:462-466)
  - [checkExtensionMembership](../c/checkExtensionMembership.md) (in src/bin/pg_dump/pg_dump.c:1744)
  - [getTableDataFKConstraints](../g/getTableDataFKConstraints.md) (in src/bin/pg_dump/pg_dump.c:3042)
  - [getDependencies](../g/getDependencies.md) (in src/bin/pg_dump/pg_dump.c:18681,18684)

## Notes and Other Information
- The function does not eliminate duplicate dependencies, which means the same dependency can be added multiple times
- Uses exponential growth strategy for memory allocation (doubles the allocation size when more space is needed)
- Initial allocation size is 16 DumpId entries, providing a reasonable balance between memory usage and reallocation frequency
- The function directly modifies the DumpableObject's nDeps, allocDeps, and dependencies fields
- This function is heavily used throughout the pg_dump codebase for establishing object relationships and dump ordering