# allocNewBuffer

## Location
src/backend/access/spgist/spgutils.c: 505 - 560

## Overview
Allocates and initializes a new buffer page of the specified type and parity for SP-GiST index operations, handling parity constraints for inner pages.

## Definition
static Buffer allocNewBuffer(Relation index, int flags)

## Detailed Description
This static function provides a higher-level interface for buffer allocation in SP-GiST indexes with specific type and parity requirements. For leaf pages, it simply allocates a new buffer without parity considerations. For inner pages, it implements a parity-checking mechanism to ensure proper index structure.

When an inner page with incorrect parity is obtained, the function doesn't simply discard it. Instead, it records the page in the lastUsedPages cache with its available free space, then releases the buffer and tries again. This optimization allows the page to potentially be reused later in the same session, or eventually be reclaimed by VACUUM for recycling through the Free Space Map.

The function deliberately does not add successfully allocated pages to the lastUsedPages cache, leaving this responsibility to the caller after they have consumed some space, ensuring more accurate free space tracking.

## Parameters / Member Variables
- : Relation object representing the SP-GiST index requiring a new buffer
- : Bit flags specifying buffer requirements (leaf vs inner, nulls handling, parity constraints)

## Dependencies
- Functions called/Symbols referenced:
  - [spgGetCache](../s/spgGetCache.md)
  - [SpGistNewBuffer](../S/SpGistNewBuffer.md)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - GBUF_REQ_LEAF
  - GBUF_REQ_NULLS
  - GBUF_INNER_PARITY
  - GBUF_PARITY_MASK
  - GBUF_NULLS
- Called from (representative examples):
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)

## Notes and Other Information
The parity mechanism is crucial for SP-GiST index structure integrity, ensuring inner pages are allocated with appropriate characteristics. Pages with wrong parity are not immediately discarded but cached for potential future use, reducing waste. The function operates in a loop until a suitable page is found, which is always guaranteed since SpGistNewBuffer can extend the index file if necessary.