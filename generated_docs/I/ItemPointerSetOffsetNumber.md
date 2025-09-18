# ItemPointerSetOffsetNumber

## Location
[src/include/storage/itemptr.h:158-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L158-L171)

## Overview
Sets only the offset number portion of a disk item pointer, leaving the block number unchanged, providing targeted modification of the intra-block position component.

## Definition


## Detailed Description
ItemPointerSetOffsetNumber is a specialized inline function that modifies only the offset number component of an existing ItemPointerData structure while preserving the current block number. This function is essential for operations that need to update the position of a tuple within a specific block without changing which block it references. It's commonly used during tuple compaction, line pointer reassignment, and various maintenance operations within individual pages.

The function provides direct access to the ip_posid field of the ItemPointerData structure, offering a clean interface for offset-only updates. This granular control is particularly valuable in heap and index operations where block-level references remain stable but intra-block positioning changes.

## Parameters / Member Variables
- : Pointer to the ItemPointerData structure to be modified (must be valid)
- : The new offset number to assign within the current block

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (assertion validation)
- Called from (representative examples):
  - gistFormTuple
  - [heap_hot_search_buffer](../h/heap_hot_search_buffer.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - GinItemPointerSetOffsetNumber
  - [BTreeTupleSetPosting](../B/BTreeTupleSetPosting.md)

## Notes and Other Information
- This is an inline function defined in itemptr.h for optimal performance
- Preserves the existing block number while updating only the offset component
- Commonly used in tuple reorganization and line pointer management
- Essential for maintaining references during page-level maintenance operations
- Provides atomic offset updates without affecting block positioning
- Frequently used in WAL replay scenarios for position adjustments