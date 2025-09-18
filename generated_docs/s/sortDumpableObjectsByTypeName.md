# sortDumpableObjectsByTypeName

## Location
src/bin/pg_dump/pg_dump_sort.c: 191 - 198

## Overview
Sorts an array of DumpableObject pointers into a type/name-based ordering, serving as the initial sorting step before dependency-based ordering in PostgreSQL's pg_dump utility.

## Definition
void sortDumpableObjectsByTypeName(DumpableObject **objs, int numObjs)

## Detailed Description
This function provides a stable, predictable ordering of database objects by sorting them first by type and then by name. It serves as the foundation for pg_dump's object ordering strategy, creating a consistent baseline before more complex dependency-based sorting is applied. The function uses the standard C library qsort function with a custom comparison function (DOTypeNameCompare) to achieve the desired ordering.

The type/name ordering ensures that objects of the same type are grouped together and sorted alphabetically within each type group, making the dump output more organized and predictable for users and tools that process PostgreSQL dumps.

## Parameters / Member Variables
- : Array of pointers to DumpableObject structures to be sorted
- : Number of objects in the array

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard C library function)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (custom comparison function)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:1013)

## Notes and Other Information
- The function only performs sorting when numObjs > 1, avoiding unnecessary work for single-object or empty arrays
- This sorting is typically the first step in pg_dump's multi-phase object ordering process
- The resulting order provides a stable baseline that helps ensure consistent dump output across different runs
- Located in src/bin/pg_dump/pg_dump_sort.c:191-198