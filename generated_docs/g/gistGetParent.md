# gistGetParent

## Location
[src/backend/access/gist/gistbuild.c:1565-1579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L1565-L1579)

## Overview
Retrieves the parent block number for a given child block from the GiST parent map during index construction.

## Definition
```c
static BlockNumber gistGetParent(GISTBuildState *buildstate, BlockNumber child)
```

## Detailed Description
This function performs a lookup in the parent map hash table to find the parent block number associated with a given child block number. It uses hash_search with the HASH_FIND action to locate the entry for the specified child block. If the entry is not found, the function raises an ERROR indicating that the parent-child relationship was not properly established, which suggests a bug in the index construction process. Upon successful lookup, it returns the parent block number stored in the entry.

## Parameters / Member Variables
- `buildstate`: Pointer to the GISTBuildState structure containing the parent map hash table
- `child`: Block number of the child page whose parent is being queried

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - ParentMapEntry
  - HASH_FIND
  - elog (for error reporting)
- Called from (representative examples):
  - [gistBufferingFindCorrectParent](gistBufferingFindCorrectParent.md)

## Notes and Other Information
- This is a static function, only accessible within the gistbuild.c file
- The function will throw an ERROR if the child block is not found in the parent map
- Returns BlockNumber type representing the parent page
- Essential for navigating the index hierarchy during buffering-based GiST construction
- The error condition should never occur in normal operation if the parent map is properly maintained
- Used primarily in scenarios where the build process needs to traverse up the index tree structure

## Simplified Source

```c
static BlockNumber
gistGetParent(GISTBuildState *buildstate, BlockNumber child)
{
    ParentMapEntry *entry;
    bool found;

    // Look up child block in parent map
    entry = (ParentMapEntry *) hash_search(buildstate->parentMap,
                                          &child,
                                          HASH_FIND,
                                          &found);

    // Child must exist in parent map
    if (!found)
        elog(ERROR, "could not find parent of block %u in lookup table", child);

    return entry->parentblkno;
}
```