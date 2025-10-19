# getDumpableObjects

## Location
[src/bin/pg_dump/common.c:786-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L786-L806)

## Overview
Builds an array of pointers to all known dumpable objects by creating a modifiable copy of the internal object mapping.

## Definition

```c
void
getDumpableObjects(DumpableObject ***objs, int *numObjs)
```
## Detailed Description
This function serves as an interface to retrieve all currently registered dumpable objects in pg_dump. It iterates through the internal  array, which maintains a mapping of dump IDs to DumpableObject pointers, and creates a compact array containing only the valid (non-NULL) object pointers. The function allocates memory for the output array and returns both the array of object pointers and the count of objects through output parameters.

The function is essential for pg_dump's operation as it provides access to the complete catalog of objects that need to be dumped, allowing other parts of the system to iterate over and process all registered objects.

## Parameters / Member Variables
- `***objs`: Output parameter - pointer to array of DumpableObject pointers that will be allocated and populated with all known dumpable objects
- `*numObjs`: Output parameter - pointer to integer that will be set to the number of objects in the returned array
## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_array (memory allocation function)
  - DumpableObject (structure type for dumpable objects)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:999)
  - [getTableDataFKConstraints](getTableDataFKConstraints.md) (in src/bin/pg_dump/pg_dump.c:3021)

## Notes and Other Information
- The function accesses global variables  and  which maintain the internal object registry
- The caller is responsible for managing the memory allocated by this function
- The function starts iteration from index 1, suggesting that index 0 may be reserved or unused in the dumpIdMap
- This is a utility function that provides a stable interface to the internal object storage mechanism

## Simplified Source

```c
void
getDumpableObjects(DumpableObject ***objs, int *numObjs)
{
    // Allocate array to hold all possible objects
    *objs = pg_malloc_array(DumpableObject *, allocedDumpIds);

    // Copy all non-NULL objects to the output array
    int j = 0;
    for (int i = 1; i < allocedDumpIds; i++) {
        if (dumpIdMap[i]) {
            (*objs)[j++] = dumpIdMap[i];
        }
    }

    // Return the actual count of objects found
    *numObjs = j;
}
```