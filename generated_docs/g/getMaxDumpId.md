# getMaxDumpId

## Location
[src/bin/pg_dump/common.c:743-753](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L743-L753)

## Overview
Returns the highest DumpId that has been assigned so far during the pg_dump process.

## Definition


## Detailed Description
This function provides read-only access to the current maximum DumpId value by returning the global lastDumpId counter. It serves as a query function that allows other parts of the pg_dump system to determine the total number of dump objects that have been assigned identifiers. This information is particularly useful for algorithms that need to allocate arrays or perform operations based on the total number of dumpable objects, such as dependency sorting and loop detection algorithms.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - lastDumpId (global variable that tracks the highest assigned DumpId)
- Data structures used:
  - DumpId (return type)
- Called from (representative examples):
  - [TopoSort](../T/TopoSort.md) (src/bin/pg_dump/pg_dump_sort.c:602)
  - [findDependencyLoops](../f/findDependencyLoops.md) (src/bin/pg_dump/pg_dump_sort.c:776-777)

## Notes and Other Information
- Simple getter function that returns the current value of lastDumpId
- Does not modify any state, providing read-only access to the maximum DumpId
- Essential for dependency sorting algorithms that need to know the total number of objects
- Used by topological sorting and cycle detection routines in pg_dump_sort.c
- The returned value represents the count of all DumpIds assigned, including both DumpableObjects and standalone DumpIds created by createDumpId()
- Critical for memory allocation and array sizing in sorting algorithms