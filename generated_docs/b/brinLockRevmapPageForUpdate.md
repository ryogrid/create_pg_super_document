# brinLockRevmapPageForUpdate

## Location
[src/backend/access/brin/brin_revmap.c:134-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L134-L154)

## Overview
Prepares a revmap page for update by obtaining and exclusively locking the buffer containing the revmap entry for a given heap block.

## Definition
```c
Buffer brinLockRevmapPageForUpdate(BrinRevmap *revmap, BlockNumber heapBlk)
```

## Detailed Description
This function obtains the appropriate revmap page buffer for a given heap block and locks it exclusively in preparation for updating the revmap entry. The function assumes that the revmap has already been extended to cover the specified heap block (typically done by calling brinRevmapExtend beforehand). It delegates the buffer acquisition to revmap_get_buffer, which handles buffer caching and ensures the correct page is loaded.

The function performs the following operations:
1. Obtains the revmap buffer for the specified heap block via revmap_get_buffer
2. Acquires an exclusive lock on the buffer to prevent concurrent modifications
3. Records the buffer in the revmap structure for later cleanup
4. Returns the locked buffer for the caller to perform updates

The returned buffer remains locked and cached in the revmap structure until the revmap operation is finished, at which point it will be automatically released.

## Parameters / Member Variables
- `revmap`: The BrinRevmap access object containing the revmap state
- `heapBlk`: The heap block number for which to lock the corresponding revmap page

## Dependencies
- Functions called/Symbols referenced:
  - [revmap_get_buffer](../r/revmap_get_buffer.md)
  - [LockBuffer](../L/LockBuffer.md)
- Types referenced:
  - [BrinRevmap](../B/BrinRevmap.md)
  - BlockNumber
  - Buffer
  - BUFFER_LOCK_EXCLUSIVE
- Called from:
  - [brin_doupdate](brin_doupdate.md)
  - [brin_doinsert](brin_doinsert.md)
  - [brinRevmapDesummarizeRange](brinRevmapDesummarizeRange.md)

## Notes and Other Information
- Callers should typically call brinRevmapExtend before this function to ensure the revmap covers the target heap block
- The function does not extend the revmap itself - it assumes adequate coverage exists
- The returned buffer is cached in revmap->rm_currBuf and will be released when the revmap operation completes
- The exclusive lock prevents concurrent modifications to the revmap page during updates
- This function is part of the critical path for BRIN index maintenance operations including insertions and updates