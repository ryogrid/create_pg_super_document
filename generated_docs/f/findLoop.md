# findLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:842-926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L842-L926)

## Overview
findLoop performs recursive depth-first search to detect circular dependency loops starting from a specific object in pg_dump's dependency graph.

## Definition

```c
static int
findLoop(DumpableObject *obj,
		 DumpId startPoint,
		 bool *processed,
		 DumpId *searchFailed,
		 DumpableObject **workspace,
		 int depth)
```
## Detailed Description
findLoop implements a recursive depth-first search algorithm to detect dependency cycles in pg_dump's object dependency graph. The function searches for a path from the given starting object that eventually leads back to the startPoint, forming a circular dependency.

The algorithm employs several optimization strategies: it skips already-processed objects to avoid finding overlapping loops, uses memoization via the searchFailed array to avoid redundant searches, and prevents infinite recursion by checking for objects already present in the current search path.

The function builds the potential loop path in the workspace array as it recurses. When it finds a direct dependency back to the startPoint, it returns the depth (loop length). If no loop is found through any outgoing dependencies, it memoizes this negative result and returns 0.

## Parameters / Member Variables
- `*obj`: The current DumpableObject being examined in the search
- `startPoint`: DumpId of the object where we want to find a cycle back to
- `*processed`: Boolean array marking objects already processed in previous loop searches
- `*searchFailed`: DumpId array for memoizing failed search paths from each object
- `**workspace`: Array being built with objects forming the potential dependency loop
- `depth`: Current number of valid entries in the workspace array
## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByDumpId](findObjectByDumpId.md)
  - [findLoop](findLoop.md) (recursive call)
- Called from (representative examples):
  - [findDependencyLoops](findDependencyLoops.md)
  - [findLoop](findLoop.md) (recursive)

## Notes and Other Information
- Returns the length of the found loop (> 0) on success, or 0 if no loop is found
- Uses multiple optimization strategies to avoid infinite recursion and redundant work
- Employs memoization to remember negative search results for efficiency
- Can find any arbitrary cycle if the starting object participates in multiple cycles
- Critical component of pg_dump's dependency loop detection and resolution system
- Guarantees workspace array won't overflow due to cycle detection in current path

## Simplified Source

```c
static int findLoop(DumpableObject *obj, DumpId startPoint,
                   bool *processed, DumpId *searchFailed,
                   DumpableObject **workspace, int depth) {
    // Skip already-processed objects to avoid overlapping loops
    if (processed[obj->dumpId])
        return 0;

    // Skip if we've already proven no path from here to startPoint
    if (searchFailed[obj->dumpId] == startPoint)
        return 0;

    // Check if object is already in current search path (prevents infinite recursion)
    for (int i = 0; i < depth; i++) {
        if (workspace[i] == obj)
            return 0;
    }

    // Add current object to the workspace path
    workspace[depth++] = obj;

    // Check if any direct dependency leads back to startPoint
    for (int i = 0; i < obj->nDeps; i++) {
        if (obj->dependencies[i] == startPoint)
            return depth;  // Found loop!
    }

    // Recursively search through all dependencies
    for (int i = 0; i < obj->nDeps; i++) {
        DumpableObject *nextobj = findObjectByDumpId(obj->dependencies[i]);
        if (!nextobj)
            continue;  // Skip undumped objects

        int newDepth = findLoop(nextobj, startPoint, processed,
                               searchFailed, workspace, depth);
        if (newDepth > 0)
            return newDepth;  // Found loop in recursive call
    }

    // No loop found - memoize this negative result
    searchFailed[obj->dumpId] = startPoint;
    return 0;
}
```