# BufferGetPageSize

## Location
src/include/storage/bufmgr.h: 393 - 403

## Overview
BufferGetPageSize is a static inline function that returns the page size within a buffer in PostgreSQLs buffer management system.

## Definition
static inline Size BufferGetPageSize(Buffer buffer)

## Detailed Description
BufferGetPageSize returns the size of a page within a buffer. Currently, this function simply returns the BLCKSZ constant, which represents the standard block size used throughout PostgreSQL. The function is designed to be extensible - the comment indicates that it should potentially dig out the page size from the buffer descriptor in the future, which would allow for variable page sizes.

The function accepts any valid buffer (both local and shared) and assumes the buffer is valid. It includes an assertion macro to verify this precondition. The buffer can contain either a formatted disk page or raw disk block data.

## Parameters / Member Variables
- buffer: Buffer identifier for which to get the page size (type: Buffer)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsValid (through AssertMacro for validation)
  - AssertMacro (assertion macro)
  - BLCKSZ (block size constant)
  - Size (return type)
- Called from (representative examples):
  - GinInitBuffer (src/backend/access/gin/ginutil.c:352)
  - hash_xlog_squeeze_page (src/backend/access/hash/hash_xlog.c:745)
  - heap_xlog_insert (src/backend/access/heap/heapam.c:9642)
  - _bt_split (src/backend/access/nbtree/nbtinsert.c:1547)
  - SpGistInitBuffer (src/backend/access/spgist/spgutils.c:716)

## Notes and Other Information
- Currently returns a fixed page size (BLCKSZ) for all buffers
- The implementation is marked with XXX comment indicating future enhancement to support variable page sizes
- Function works with both formatted disk pages and raw disk blocks
- Used extensively throughout PostgreSQL access methods for buffer size calculations
- The function assumes buffer validity and will assert if buffer is invalid
- Future versions may extract page size from buffer descriptor to support variable-sized pages