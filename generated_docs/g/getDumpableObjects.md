# getDumpableObjects

## Location
src/bin/pg_dump/common.c: 786 - 806

## Overview
Builds an array of pointers to all known dumpable objects by creating a modifiable copy of the internal object mapping.

## Definition


## Detailed Description
This function serves as an interface to retrieve all currently registered dumpable objects in pg_dump. It iterates through the internal  array, which maintains a mapping of dump IDs to DumpableObject pointers, and creates a compact array containing only the valid (non-NULL) object pointers. The function allocates memory for the output array and returns both the array of object pointers and the count of objects through output parameters.

The function is essential for pg_dump's operation as it provides access to the complete catalog of objects that need to be dumped, allowing other parts of the system to iterate over and process all registered objects.

## Parameters / Member Variables
- : Output parameter - pointer to array of DumpableObject pointers that will be allocated and populated with all known dumpable objects
- : Output parameter - pointer to integer that will be set to the number of objects in the returned array

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_array (memory allocation function)
  - DumpableObject (structure type for dumpable objects)
- Called from (representative examples):
  - main (in src/bin/pg_dump/pg_dump.c:999)
  - getTableDataFKConstraints (in src/bin/pg_dump/pg_dump.c:3021)

## Notes and Other Information
- The function accesses global variables  and  which maintain the internal object registry
- The caller is responsible for managing the memory allocated by this function
- The function starts iteration from index 1, suggesting that index 0 may be reserved or unused in the dumpIdMap
- This is a utility function that provides a stable interface to the internal object storage mechanism