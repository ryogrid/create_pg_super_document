# ginHeapTupleInsert

## Location
src/backend/access/gin/gininsert.c: 465 - 482

## Overview
The ginHeapTupleInsert function handles the insertion of index entries for a single indexable item during normal (non-fast-update) insertion operations in GIN indexes.

## Definition


## Detailed Description
The ginHeapTupleInsert function is a helper function that processes a single heap tuple value for insertion into a GIN index. It works by:

1. **Entry Extraction**: Calls ginExtractEntries to extract all indexable entries from the given value using the appropriate extraction function for the attribute
2. **Individual Entry Insertion**: Iterates through each extracted entry and calls ginEntryInsert to insert it into the index structure
3. **Category Handling**: Properly manages GIN null categories for each extracted entry

This function is designed for "normal" insertion mode, as opposed to fast-update mode where entries are initially stored in a pending list. It directly inserts entries into the main index structure.

## Parameters / Member Variables
- : Pointer to the GIN state structure containing index metadata and operator information
- : The attribute (column) number being indexed (1-based offset)
- : The actual data value to be indexed
- : Boolean flag indicating whether the value is NULL
- : ItemPointer referencing the heap tuple location (TID) that contains this value

## Dependencies
- Functions called/Symbols referenced:
  - ginExtractEntries
  - ginEntryInsert
  - GinNullCategory
- Called from (representative examples):
  - gininsert

## Notes and Other Information
- This is a static function, only visible within gininsert.c
- Used specifically for normal insertion mode, not fast-update mode
- The function handles multiple entries that can be extracted from a single value (e.g., array elements, text tokens)
- Each extracted entry is inserted individually with its corresponding category information
- The item parameter (heap TID) is passed to each ginEntryInsert call to establish the connection between index entries and heap tuples
- Does not handle statistics collection (NULL is passed to ginEntryInsert for stats parameter)