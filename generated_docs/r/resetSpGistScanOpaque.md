# resetSpGistScanOpaque

## Location
src/backend/access/spgist/spgscan.c: 154 - 207

## Overview
Resets the SP-GiST scan opaque structure and initializes the search queue to start scanning from the root page, clearing any previously active scan state.

## Definition
```c
static void resetSpGistScanOpaque(SpGistScanOpaque so)
```

## Detailed Description
This static function performs a complete reset of the SP-GiST scan state stored in the SpGistScanOpaque structure. It resets the traversal memory context to free any accumulated memory, reinitializes the search queue using a pairing heap for distance-ordered scans, and adds work items to scan both null and non-null index entries based on the scan configuration.

The function also performs cleanup for distance-ordered scans by freeing previously allocated distances arrays and reconstructed tuples to prevent memory leaks. After cleanup, it resets the scan position pointers to start fresh.

## Parameters / Member Variables
- `so`: SpGistScanOpaque structure containing the scan state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - pairingheap_allocate
  - [pairingheap_SpGistSearchItem_cmp](../p/pairingheap_SpGistSearchItem_cmp.md)
  - [spgAddStartItem](../s/spgAddStartItem.md)
  - [pfree](../p/pfree.md) (indirectly)
- Called from:
  - [spgrescan](../s/spgrescan.md) (src/backend/access/spgist/spgscan.c:422)

## Notes and Other Information
- This is a static function internal to the spgscan.c module
- Handles both null and non-null search scenarios through searchNulls and searchNonNulls flags
- Properly manages memory cleanup for distance-ordered scans and reconstructed tuples to prevent memory leaks
- Resets the scan to its initial state, allowing for scan reuse without creating a new scan structure
- Uses a pairing heap data structure for efficient distance-ordered search operations