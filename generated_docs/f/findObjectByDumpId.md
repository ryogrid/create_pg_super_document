# findObjectByDumpId

## Location
[src/bin/pg_dump/common.c:754-766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L754-L766)

## Overview
Retrieves a DumpableObject by its unique DumpId, providing safe lookup with bounds checking.

## Definition


## Detailed Description
This function performs a lookup operation to find a DumpableObject associated with a given DumpId. It uses the global dumpIdMap array to perform the lookup, with built-in bounds checking to ensure the DumpId is valid. The function returns NULL for invalid or out-of-range DumpIds, making it safe to use in situations where the existence of an object is uncertain. This is a critical function in pg_dump's object management system, enabling efficient retrieval of objects during dependency resolution, archive building, and dump output generation.

## Parameters / Member Variables
- : The DumpId to look up; must be a positive integer within the valid range

## Dependencies
- Functions called/Symbols referenced:
  - dumpIdMap (global array that maps DumpIds to DumpableObject pointers)
  - allocedDumpIds (global variable tracking the size of dumpIdMap)
- Data structures used:
  - DumpId (parameter type)
  - DumpableObject (return type)
- Called from (representative examples):
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md) (src/bin/pg_dump/pg_dump.c:5609)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10657)
  - [dumpExtension](../d/dumpExtension.md) (src/bin/pg_dump/pg_dump.c:10878)
  - [dumpConstraint](../d/dumpConstraint.md) (src/bin/pg_dump/pg_dump.c:17264)
  - [BuildArchiveDependencies](../B/BuildArchiveDependencies.md) (src/bin/pg_dump/pg_dump.c:18858)
  - [findDumpableDependencies](findDumpableDependencies.md) (src/bin/pg_dump/pg_dump.c:18922)
  - [findLoop](findLoop.md) (src/bin/pg_dump/pg_dump_sort.c:896)

## Notes and Other Information
- Returns NULL for invalid DumpIds (≤ 0 or ≥ allocedDumpIds)
- Provides O(1) lookup time using direct array indexing
- Essential for dependency resolution and object relationship traversal
- Safe to use with potentially invalid DumpIds due to built-in bounds checking
- May return NULL for valid DumpIds that correspond to standalone entries created by createDumpId()
- Critical component of pg_dump's object tracking and retrieval infrastructure