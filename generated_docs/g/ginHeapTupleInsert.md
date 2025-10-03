# ginHeapTupleInsert

## Location
[src/backend/access/gin/gininsert.c:465-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L465-L482)

## Overview
The ginHeapTupleInsert function handles the insertion of index entries for a single indexable item during normal (non-fast-update) insertion operations in GIN indexes.

## Definition

```c
static void
ginHeapTupleInsert(GinState *ginstate, OffsetNumber attnum,
				   Datum value, bool isNull,
				   ItemPointer item)
```
## Detailed Description
The ginHeapTupleInsert function is a helper function that processes a single heap tuple value for insertion into a GIN index. It works by:

1. **Entry Extraction**: Calls ginExtractEntries to extract all indexable entries from the given value using the appropriate extraction function for the attribute
2. **Individual Entry Insertion**: Iterates through each extracted entry and calls ginEntryInsert to insert it into the index structure
3. **Category Handling**: Properly manages GIN null categories for each extracted entry

This function is designed for "normal" insertion mode, as opposed to fast-update mode where entries are initially stored in a pending list. It directly inserts entries into the main index structure.

## Parameters / Member Variables
- `*ginstate`: Pointer to the GIN state structure containing index metadata and operator information
- `attnum`: The attribute (column) number being indexed (1-based offset)
- `value`: The actual data value to be indexed
- `isNull`: Boolean flag indicating whether the value is NULL
- `item`: ItemPointer referencing the heap tuple location (TID) that contains this value
## Dependencies
- Functions called/Symbols referenced:
  - [ginExtractEntries](ginExtractEntries.md)
  - [ginEntryInsert](ginEntryInsert.md)
  - GinNullCategory
- Called from (representative examples):
  - [gininsert](gininsert.md)

## Notes and Other Information
- This is a static function, only visible within gininsert.c
- Used specifically for normal insertion mode, not fast-update mode
- The function handles multiple entries that can be extracted from a single value (e.g., array elements, text tokens)
- Each extracted entry is inserted individually with its corresponding category information
- The item parameter (heap TID) is passed to each ginEntryInsert call to establish the connection between index entries and heap tuples
- Does not handle statistics collection (NULL is passed to ginEntryInsert for stats parameter)

## Simplified Source

```c
static void
ginHeapTupleInsert(GinState *ginstate, OffsetNumber attnum,
                   Datum value, bool isNull,
                   ItemPointer item)
{
    Datum *entries;
    GinNullCategory *categories;
    int32 nentries;

    // Extract all entries from the value using opclass extract function
    entries = ginExtractEntries(ginstate, attnum, value, isNull,
                                &nentries, &categories);

    // Insert each extracted entry into the index
    for (int i = 0; i < nentries; i++) {
        ginEntryInsert(ginstate, attnum, entries[i], categories[i],
                       item, 1, NULL);
    }
}
```