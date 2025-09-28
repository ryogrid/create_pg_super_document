# PageSetChecksumCopy

## Location
[src/backend/storage/page/bufpage.c:1510-1541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L1510-L1541)

## Overview
Creates a copy of a page with a calculated checksum for safe writing to storage, protecting against concurrent modifications that could invalidate the checksum.

## Definition

```c
char *
PageSetChecksumCopy(Page page, BlockNumber blkno)
```
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
  - [PageIsNew](PageIsNew.md)
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md)
  - [MemoryContextAllocAligned](../M/MemoryContextAllocAligned.md)
  - [pg_checksum_page](../p/pg_checksum_page.md)
- Called from (representative examples):
  - [FlushBuffer](../F/FlushBuffer.md) (buffer manager page writing)

## Notes and Other Information
- Returns original page pointer if checksums are disabled or page is new
- Uses statically-allocated memory that gets reused across calls
- Caller must immediately use the returned pointer and not reference it later
- Memory is allocated in TopMemoryContext for process lifetime
- Buffer is aligned to PG_IO_ALIGN_SIZE for optimal I/O performance  
- Essential for data integrity in systems with concurrent access to shared buffers
- Prevents race conditions between checksum calculation and hint bit modifications
- Part of PostgreSQL's comprehensive page corruption detection system

## Simplified Source

```c
// Simplified version of PageSetChecksumCopy
char *
PageSetChecksumCopy(Page page, BlockNumber blkno)
{
    static char *pageCopy = NULL;

    // Early exit: return original page if checksums not needed
    if (PageIsNew(page) || !DataChecksumsEnabled())
        return (char *) page;

    // Lazy allocation: allocate aligned buffer on first use
    if (pageCopy == NULL)
        pageCopy = MemoryContextAllocAligned(TopMemoryContext,
                                           BLCKSZ,
                                           PG_IO_ALIGN_SIZE,
                                           0);

    // Create safe copy and calculate checksum
    memcpy(pageCopy, (char *) page, BLCKSZ);
    ((PageHeader) pageCopy)->pd_checksum = pg_checksum_page(pageCopy, blkno);

    return pageCopy;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential explanations
- Consolidated the core logic flow into clear steps
- Maintained all essential functionality and error checking
- Simplified variable declarations and alignment
- Preserved the critical race condition protection mechanism