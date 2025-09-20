# vm_extend

## Location
[src/backend/access/heap/visibilitymap.c:612-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L612-L632)

## Overview
Extends the visibility map fork to ensure it has at least the specified number of blocks, creating zeroed pages as needed.

## Definition

```c
static Buffer
vm_extend(Relation rel, BlockNumber vm_nblocks)
```
## Detailed Description
This static function extends the visibility map fork to ensure it contains at least vm_nblocks blocks. It uses ExtendBufferedRelTo() with flags to create the fork if it doesn't exist and clear the size cache. The function handles the extension atomically and returns a buffer for the last block. After extending, it sends a shared invalidation message to force other backends to close their smgr references, which is an optimization to avoid repeated existence/size checks since visibility map extensions are infrequent.

## Parameters / Member Variables
- : The relation whose visibility map fork should be extended
- : The minimum number of blocks the visibility map should contain

## Dependencies
- Functions called/Symbols referenced:
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md)
  - [CacheInvalidateSmgr](../C/CacheInvalidateSmgr.md)
  - RelationGetSmgr
  - BMR_REL
  - VISIBILITYMAP_FORKNUM
  - EB_CREATE_FORK_IF_NEEDED
  - EB_CLEAR_SIZE_CACHE
  - RBM_ZERO_ON_ERROR
- Called from (representative examples):
  - [vm_readbuf](vm_readbuf.md)

## Notes and Other Information
- Static function internal to visibilitymap.c
- Creates the visibility map fork if it doesn't exist yet
- Uses shared invalidation messages as an optimization to avoid repeated file system checks
- Returns a buffer for the last block in the extended range
- All new blocks are initialized with zeros through RBM_ZERO_ON_ERROR flag
- Part of PostgreSQL's visibility map infrastructure for efficient page visibility tracking