# findDumpableDependencies

## Location
[src/bin/pg_dump/pg_dump.c:18886-18941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18886-L18941)

## Overview
A recursive function that searches through DumpableObject dependencies to build a list of dependencies that will actually be included in the dump, resolving dependency chains through non-dumped objects.

## Definition


## Detailed Description
This function recursively traverses the dependency tree of a DumpableObject to identify all dependencies that should be included in the final dump. When an object depends on something that won't be dumped, the function recurses into that object's dependencies to find transitive dependencies that will be dumped. It dynamically grows the dependencies array as needed and avoids infinite recursion by relying on sortDumpableObjects having already broken dependency cycles.

## Parameters / Member Variables
- : ArchiveHandle pointer for the archive being processed
- : The DumpableObject whose dependencies are being analyzed
- : Pointer to array of DumpId values representing dependencies (modified)
- : Pointer to count of dependencies found so far (modified)
- : Pointer to allocated size of dependencies array (modified)

## Dependencies
- Functions called/Symbols referenced:
  - [TocIDRequired](../T/TocIDRequired.md)
  - [findObjectByDumpId](findObjectByDumpId.md)
  - pg_realloc
  - [findDumpableDependencies](findDumpableDependencies.md) (recursive call)
- Types used:
  - DumpableObject
  - DumpId
  - DO_PRE_DATA_BOUNDARY
  - DO_POST_DATA_BOUNDARY
- Called from (representative examples):
  - [BuildArchiveDependencies](../B/BuildArchiveDependencies.md)
  - [findDumpableDependencies](findDumpableDependencies.md) (recursive)

## Notes and Other Information
- Ignores section boundary objects (DO_PRE_DATA_BOUNDARY, DO_POST_DATA_BOUNDARY) to avoid bogus dependencies
- Doubles the allocation size when the dependencies array needs to grow
- Relies on sortDumpableObjects having broken dependency cycles to prevent infinite recursion
- Critical helper function for BuildArchiveDependencies in resolving transitive dependencies