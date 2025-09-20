# vm_readbuf

## Location
[src/backend/access/heap/visibilitymap.c:538-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L538-L611)

## Overview
Reads a visibility map page from storage, optionally extending the file if the page doesn't exist and handling page initialization.

## Definition

```c
static Buffer
vm_readbuf(Relation rel, BlockNumber blkno, bool extend)
```
## Detailed Description
This static function is responsible for reading visibility map pages from storage. It manages the cached visibility map fork size and handles cases where the requested block doesn't exist yet. The function uses RBM_ZERO_ON_ERROR mode for robust reading, preferring to clear corrupt pages rather than error out. When extending is requested and the block doesn't exist, it calls vm_extend() to grow the file. The function also handles concurrent page initialization scenarios by using double-checked locking pattern to ensure pages are properly initialized exactly once.

## Parameters / Member Variables
- : The relation whose visibility map page should be read
- : The block number of the visibility map page to read
- : If true, extend the visibility map file if the page doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetSmgr
  - [smgrexists](../s/smgrexists.md)
  - smgrnblocks
  - [vm_extend](vm_extend.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [PageIsNew](../P/PageIsNew.md)
  - [LockBuffer](../L/LockBuffer.md)
  - PageInit
  - [BufferGetPage](../B/BufferGetPage.md)
  - SMgrRelation
  - VISIBILITYMAP_FORKNUM
  - RBM_ZERO_ON_ERROR
  - BUFFER_LOCK_EXCLUSIVE
  - BUFFER_LOCK_UNLOCK
- Called from (representative examples):
  - [visibilitymap_pin](visibilitymap_pin.md)
  - [visibilitymap_get_status](visibilitymap_get_status.md)
  - [visibilitymap_count](visibilitymap_count.md)
  - [visibilitymap_prepare_truncate](visibilitymap_prepare_truncate.md)

## Notes and Other Information
- Static function internal to visibilitymap.c
- Uses cached block count information to avoid unnecessary system calls
- Implements double-checked locking for page initialization to handle concurrency
- Safe for callers that don't inspect page headers without locks since PageGetContents() doesn't require correct headers
- Returns InvalidBuffer if the page doesn't exist and extend is false