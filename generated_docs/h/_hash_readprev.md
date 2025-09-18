# _hash_readprev

## Location
[src/backend/access/hash/hashsearch.c:197-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsearch.c#L197-L287)

## Overview
Moves to the previous page in a hash bucket during backward scanning, with special handling for bucket splits and buffer management.

## Definition
```c
static void _hash_readprev(IndexScanDesc scan, Buffer *bufp, Page *pagep, HashPageOpaque *opaquep)
```

## Detailed Description
This function navigates backward through a hash bucket chain during reverse index scanning. It handles the complex scenario where a bucket split is in progress, allowing scans to transition between the split bucket and the populated bucket when moving backward. The function manages buffer pins carefully, maintaining pins on primary bucket pages while properly handling overflow page buffers.

When reaching the beginning of a split bucket during backward scan, the function transitions to scanning the populated bucket and positions at the end of its chain. The function also ensures proper buffer management by avoiding double-pinning of bucket pages and implements interrupt checking for responsiveness.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and relation information  
- `bufp`: Pointer to Buffer being updated to point to the previous page
- `pagep`: Pointer to Page being updated to point to the previous page content
- `opaquep`: Pointer to HashPageOpaque being updated with previous page's opaque data

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumberIsValid
  - [_hash_getbuf](_hash_getbuf.md)
  - [_hash_relbuf](_hash_relbuf.md)
  - [_hash_dropbuf](_hash_dropbuf.md)
  - [_hash_readnext](_hash_readnext.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - HashPageGetOpaque
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [_hash_readpage](_hash_readpage.md)

## Notes and Other Information
The function maintains pins on primary bucket pages throughout the scan operation for efficiency. During bucket splits in backward scans, it handles the transition from split bucket to populated bucket by moving to the end of the populated bucket chain. The hashso_buc_split flag tracks scan state transitions. Buffer management includes special logic to avoid double-pinning when encountering bucket pages that already have maintained pins.