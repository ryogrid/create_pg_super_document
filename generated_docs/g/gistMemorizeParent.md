# gistMemorizeParent

## Location
[src/backend/access/gist/gistbuild.c:1528-1543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L1528-L1543)

## Overview
Records a parent-child relationship between two blocks in the GiST index parent map during index construction.

## Definition
```c
static void gistMemorizeParent(GISTBuildState *buildstate, BlockNumber child, BlockNumber parent)
```

## Detailed Description
This function stores a mapping from a child block number to its parent block number in the parent map hash table. It uses the hash_search function with HASH_ENTER action to either find an existing entry for the child block or create a new one. The function then sets the parentblkno field of the entry to the specified parent block number. This mapping is crucial for maintaining the hierarchical structure of the GiST index during the buffering-based build process.

## Parameters / Member Variables
- `buildstate`: Pointer to the GISTBuildState structure containing the parent map hash table
- `child`: Block number of the child page whose parent is being recorded
- `parent`: Block number of the parent page to be associated with the child

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - ParentMapEntry
  - HASH_ENTER
- Called from (representative examples):
  - [gistProcessItup](gistProcessItup.md)
  - [gistbufferinginserttuples](gistbufferinginserttuples.md)
  - [gistMemorizeAllDownlinks](gistMemorizeAllDownlinks.md)

## Notes and Other Information
- This is a static function, only accessible within the gistbuild.c file
- The function always overwrites any existing parent mapping for the given child block
- Uses HASH_ENTER which will create a new entry if one doesn't exist for the child block
- The `found` variable is used by hash_search but not utilized by this function
- Essential for tracking page relationships during the GiST buffering build algorithm