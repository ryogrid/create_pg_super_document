# PageSetChecksumCopy

## Location
src/backend/storage/page/bufpage.c: 1510 - 1541

## Overview
Creates a copy of a page with a calculated checksum for safe writing to storage, protecting against concurrent modifications that could invalidate the checksum.

## Definition


## Detailed Description
PageSetChecksumCopy provides a thread-safe mechanism for preparing pages to be written to disk with valid checksums. The function addresses the critical problem of concurrent modifications (such as hint bit updates) that could occur between checksum calculation and actual I/O, which would result in checksum validation failures on subsequent reads.

The function uses several optimization strategies:
1. **Early exit**: Returns the original page pointer if checksums are disabled or the page is uninitialized
2. **Static allocation**: Uses a single, statically-allocated aligned buffer that is reused across calls
3. **Proper alignment**: Ensures the copy buffer meets alignment requirements for efficient checksumming
4. **Lazy allocation**: Only allocates memory when actually needed, avoiding waste in processes that never use checksums

The returned pointer must be used immediately for I/O operations and cannot be retained, as subsequent calls will overwrite the same static buffer.

## Parameters / Member Variables
- : The original page to be copied and checksummed
- : The block number of the page, used in checksum calculation

## Dependencies
- Functions called/Symbols referenced:
  - PageIsNew
  - DataChecksumsEnabled
  - MemoryContextAllocAligned
  - pg_checksum_page
- Called from (representative examples):
  - FlushBuffer (buffer manager page writing)

## Notes and Other Information
- Returns original page pointer if checksums are disabled or page is new
- Uses statically-allocated memory that gets reused across calls
- Caller must immediately use the returned pointer and not reference it later
- Memory is allocated in TopMemoryContext for process lifetime
- Buffer is aligned to PG_IO_ALIGN_SIZE for optimal I/O performance  
- Essential for data integrity in systems with concurrent access to shared buffers
- Prevents race conditions between checksum calculation and hint bit modifications
- Part of PostgreSQL's comprehensive page corruption detection system