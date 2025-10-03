# ginInsertBAEntry

## Location
[src/backend/access/gin/ginbulk.c:148-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbulk.c#L148-L209)

## Overview
Inserts a single entry (key-value pair with heap pointer) into the BuildAccumulator's red-black tree during GIN index bulk loading operations.

## Definition
```c
static void ginInsertBAEntry(BuildAccumulator *accum, ItemPointer heapptr, OffsetNumber attnum, Datum key, GinNullCategory category)
```

## Detailed Description
This function processes a single indexed entry during GIN bulk loading by inserting it into the BuildAccumulator's red-black tree. It creates a temporary GinEntryAccumulator with the provided key, attribute number, and category, then uses rbt_insert to add it to the tree. For new entries, it initializes the complete structure including making permanent copies of non-null keys using getDatumCopy, allocating space for item pointers, and tracking memory usage. For existing entries, the tree's ginCombineData callback handles merging the new item pointer with the existing entry.

## Parameters / Member Variables
- `accum`: Pointer to BuildAccumulator containing the red-black tree and state
- `heapptr`: Pointer to the heap tuple that contains this indexed value
- `attnum`: Attribute number (1-based) of the indexed column
- `key`: The indexed key value (Datum)
- `category`: Category of the key (normal, null, empty, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [rbt_insert](../r/rbt_insert.md)
  - [getDatumCopy](getDatumCopy.md)
  - [palloc](../p/palloc.md)
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - GIN_CAT_NORM_KEY (constant)
  - DEF_NPTR (constant)
  - [GinEntryAccumulator](../G/GinEntryAccumulator.md) (struct)
  - [BuildAccumulator](../B/BuildAccumulator.md) (struct)
  - [ItemPointerData](../I/ItemPointerData.md) (struct)
- Called from (representative examples):
  - [ginInsertBAEntries](ginInsertBAEntries.md)

## Notes and Other Information
- Static function used internally within the GIN bulk loading module
- Handles both new entry creation and merging with existing entries
- Uses getDatumCopy for memory-tracked copying of non-null keys
- Allocates initial space for DEF_NPTR item pointers per entry
- Tracks all allocated memory in the BuildAccumulator for monitoring
- Part of the GIN access method's bulk loading entry accumulation system

## Simplified Source

```c
static void ginInsertBAEntry(BuildAccumulator *accum, ItemPointer heapptr,
                            OffsetNumber attnum, Datum key, GinNullCategory category) {
    // Set up temporary entry for tree lookup/insertion
    GinEntryAccumulator temp_entry;
    temp_entry.attnum = attnum;
    temp_entry.key = key;
    temp_entry.category = category;
    temp_entry.list = heapptr; // temporary single-entry list

    // Try to insert into red-black tree
    bool isNew;
    GinEntryAccumulator *entry = (GinEntryAccumulator *)
        rbt_insert(accum->tree, (RBTNode *) &temp_entry, &isNew);

    if (isNew) {
        // New entry - initialize the complete structure
        if (category == GIN_CAT_NORM_KEY) {
            entry->key = getDatumCopy(accum, attnum, key); // track memory
        }

        // Set up item pointer list
        entry->maxcount = DEF_NPTR;
        entry->count = 1;
        entry->shouldSort = false;
        entry->list = (ItemPointerData *) palloc(sizeof(ItemPointerData) * DEF_NPTR);
        entry->list[0] = *heapptr;

        // Track allocated memory
        accum->allocatedMemory += GetMemoryChunkSpace(entry->list);
    }
    // For existing entries, ginCombineData callback handles merging
}
```