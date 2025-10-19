# sortDumpableObjects

## Location
[src/bin/pg_dump/pg_dump_sort.c:545-596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L545-L596)

## Overview
The main function that sorts database objects into a safe dump order using dependency analysis and topological sorting, ensuring that objects are dumped in an order that respects their interdependencies.

## Definition
void sortDumpableObjects(DumpableObject **objs, int numObjs, DumpId preBoundaryId, DumpId postBoundaryId)

## Detailed Description
This function implements the core dependency-aware sorting algorithm for PostgreSQL's pg_dump utility. It takes an array of database objects and arranges them in an order that respects their dependencies, ensuring that referenced objects are dumped before the objects that depend on them.

The function operates through the following key steps:
1. **Boundary Setup**: Stores PRE_DATA_BOUNDARY and POST_DATA_BOUNDARY object IDs in static variables for use by subsidiary functions
2. **Memory Allocation**: Creates a temporary ordering array to hold the sorted results
3. **Topological Sorting**: Repeatedly calls TopoSort to attempt dependency-based ordering
4. **Loop Resolution**: When dependency loops are detected (TopoSort returns false), calls findDependencyLoops to identify and resolve circular dependencies
5. **Result Application**: Copies the final sorted order back to the original array
6. **Cleanup**: Releases the temporary ordering array

The function handles the complex challenge of dependency loops, which can occur due to circular references between database objects. When such loops are detected, it employs sophisticated loop-breaking algorithms to produce a valid ordering while minimizing the impact on restore correctness.

## Parameters / Member Variables
- : Array of pointers to DumpableObject structures to be sorted
- : Number of objects in the array
- : DumpId of the PRE_DATA_BOUNDARY object for dependency calculations
- : DumpId of the POST_DATA_BOUNDARY object for dependency calculations

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation)
  - [TopoSort](../T/TopoSort.md) (topological sorting algorithm)
  - [findDependencyLoops](../f/findDependencyLoops.md) (dependency loop detection and resolution)
  - memcpy (standard memory copy)
  - free (standard memory deallocation)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:1015)

## Notes and Other Information
- Returns early if numObjs <= 0, though this condition should not occur in normal operation
- Uses static variables to store boundary IDs, which is acknowledged as "grotty" but preferred over extensive parameter passing
- The function may iterate multiple times through the TopoSort/findDependencyLoops cycle until a valid ordering is achieved
- Critical for ensuring that pg_dump output can be successfully restored without dependency violations
- Part of the sophisticated multi-phase sorting system that begins with type/name ordering and culminates in dependency-aware ordering
- Located in src/bin/pg_dump/pg_dump_sort.c:545-596

## Simplified Source

```c
void sortDumpableObjects(DumpableObject **objs, int numObjs,
                        DumpId preBoundaryId, DumpId postBoundaryId) {
    // Early exit for empty arrays
    if (numObjs <= 0)
        return;

    // Store boundary IDs in static variables for subsidiary functions
    preDataBoundId = preBoundaryId;
    postDataBoundId = postBoundaryId;

    // Allocate temporary array for sorting results
    DumpableObject **ordering = pg_malloc(numObjs * sizeof(DumpableObject *));
    int nOrdering;

    // Keep trying topological sort until all dependency loops are resolved
    while (!TopoSort(objs, numObjs, ordering, &nOrdering)) {
        findDependencyLoops(ordering, nOrdering, numObjs);
    }

    // Copy sorted order back to original array
    memcpy(objs, ordering, numObjs * sizeof(DumpableObject *));

    // Clean up temporary array
    free(ordering);
}
```