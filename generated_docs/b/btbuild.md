# btbuild

## Location
[src/backend/access/nbtree/nbtsort.c:293-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L293-L362)

## Overview
The main entry point function for building a new B-tree index from scratch, coordinating the entire index construction process including heap scanning, sorting, and tree structure creation.

## Definition

```c
IndexBuildResult *
btbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```
## Detailed Description
 is the primary function responsible for constructing a new B-tree index. It orchestrates the complete index building process through several distinct phases:

1. **Initialization**: Sets up the build state structure with index properties (uniqueness, null handling) and initializes spool structures for temporary storage.

2. **Heap Scanning**: Calls  to scan the heap relation and collect index tuples into temporary spool files while sorting them.

3. **Tree Construction**: Uses  to sort the collected tuples and build the actual B-tree structure, creating leaf pages and upper-level internal pages.

4. **Cleanup**: Destroys temporary spool structures and handles parallel processing cleanup if applicable.

The function includes validation to ensure the target index relation is empty before beginning construction. It also provides optional statistics collection for performance monitoring when compiled with BTREE_BUILD_STATS.

## Parameters
- : The source heap relation from which to build the index
- : The target index relation to be populated  
- : Metadata about the index including uniqueness constraints and null handling rules

## Dependencies
- Functions called/Symbols referenced:
  -  - Scans heap and populates spool files
  -  - Builds the actual B-tree structure
  -  - Cleans up temporary spool files
  -  - Handles parallel processing cleanup
  -  - Validates index is empty
  - , ,  - Data structures
- Called from:
  -  - B-tree access method handler

## Notes and Other Information
- This function expects to be called exactly once per index relation and will error if the index already contains data
- Supports both unique and non-unique indexes with configurable null handling
- Can utilize parallel processing for large indexes when appropriate
- Returns statistics about the build process including tuple counts from both heap and index
- Performance statistics are conditionally compiled and logged when BTREE_BUILD_STATS is enabled