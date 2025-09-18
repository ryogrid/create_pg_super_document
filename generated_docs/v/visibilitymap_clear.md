# visibilitymap_clear

## Location
src/backend/access/heap/visibilitymap.c: 138 - 190

## Overview
Clears specified visibility map bits for a single heap page, marking the page as no longer having certain visibility properties like all-visible or all-frozen status.

## Definition


## Detailed Description
The visibilitymap_clear function removes visibility map bits for a given heap block. It operates on a pre-pinned visibility map buffer and clears the bits specified by the flags parameter. The function performs an exclusive lock on the buffer during the operation and marks the buffer as dirty if any bits were actually cleared. This function is critical for maintaining consistency between the heap and its visibility map during DML operations.

The function includes safety assertions to prevent invalid bit combinations, specifically ensuring that the all_visible bit cannot be cleared while leaving the all_frozen bit set, which would create an inconsistent state.

## Parameters / Member Variables
- : The relation whose visibility map is being modified
- : Block number of the heap page whose visibility bits are being cleared
- : Pre-pinned buffer containing the correct visibility map page
- : Bitmask specifying which visibility bits to clear (must include valid bits)

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK (macro for converting heap block to map block)
  - HEAPBLK_TO_MAPBYTE (macro for converting heap block to map byte)  
  - HEAPBLK_TO_OFFSET (macro for converting heap block to bit offset)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number from buffer)
  - [PageGetContents](../P/PageGetContents.md) (gets page contents from buffer page)
  - VISIBILITYMAP_VALID_BITS (constant defining valid visibility bits)
  - VISIBILITYMAP_ALL_VISIBLE (constant for all-visible bit)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md) (clears bits when inserting new tuples)
  - [heap_delete](../h/heap_delete.md) (clears bits when deleting tuples)
  - [heap_update](../h/heap_update.md) (clears bits when updating tuples)
  - [lazy_scan_prune](../l/lazy_scan_prune.md) (clears bits during vacuum operations)

## Notes and Other Information
- Must be called with a properly pinned visibility map buffer obtained via visibilitymap_pin
- Returns true if any bits were actually cleared, false if the bits were already clear
- Performs exclusive locking during the operation to ensure consistency
- Includes debug tracing when TRACE_VISIBILITYMAP is defined
- Critical for maintaining heap-visibility map consistency during write operations