# findDependencyLoops

## Location
src/bin/pg_dump/pg_dump_sort.c: 746 - 841

## Overview
findDependencyLoops identifies dependency loops in pg_dump's topological sort failure output and orchestrates their repair to enable successful object sorting.

## Definition


## Detailed Description
findDependencyLoops is a critical component of pg_dump's dependency resolution system that handles cases where the initial topological sort fails due to circular dependencies. When TopoSort cannot produce a valid ordering, this function systematically searches through the problematic objects to identify dependency loops.

The function employs an efficient strategy to find and repair multiple disjoint loops in a single pass, maximizing the repair work done before attempting another topological sort. It uses three key data structures: a processed array to track already-examined objects, a searchFailed array to optimize repeated searches by memoizing negative results, and a workspace array to build candidate loop sequences.

For each unprocessed object, the function calls findLoop() to detect cycles starting from that object. When a loop is found, it immediately calls repairDependencyLoop() to break the cycle and marks all loop members as processed to avoid redundant work.

## Parameters / Member Variables
- : Array of DumpableObject pointers that TopoSort failed to sort due to circular dependencies
- : Number of objects in the objs array that need loop detection
- : Total number of objects in the entire dump universe (used for workspace sizing)

## Dependencies
- Functions called/Symbols referenced:
  - getMaxDumpId
  - pg_malloc0
  - pg_malloc
  - findLoop
  - repairDependencyLoop
  - pg_fatal
- Called from (representative examples):
  - sortDumpableObjects

## Notes and Other Information
- Uses efficient memoization in searchFailed[] array to avoid O(N^2) zeroing overhead
- Processes disjoint loops safely in parallel but handles overlapping loops sequentially
- Allocates workspace large enough for worst-case scenario of single dependency chain
- Guarantees to fix at least one loop or terminates with fatal error
- Critical for pg_dump's ability to handle complex database schemas with circular dependencies
- Part of pg_dump's multi-pass dependency resolution strategy