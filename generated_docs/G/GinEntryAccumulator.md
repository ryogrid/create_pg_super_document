# GinEntryAccumulator

## Location
src/include/access/gin_private.h: 419 - 429

## Overview
A structure used during GIN (Generalized Inverted Index) bulk insertion to accumulate item pointers for each unique key value before building the final index structure.

## Definition
```c
typedef struct GinEntryAccumulator
{
    RBTNode         rbtnode;
    Datum           key;
    GinNullCategory category;
    OffsetNumber    attnum;
    bool            shouldSort;
    ItemPointerData *list;
    uint32          maxcount;       /* allocated size of list[] */
    uint32          count;          /* current number of list[] entries */
} GinEntryAccumulator;
```

## Detailed Description
GinEntryAccumulator is a key component of PostgreSQL's GIN index bulk loading mechanism. It serves as a node in a red-black tree that accumulates item pointers (TIDs - tuple identifiers) for each unique combination of attribute number, key value, and null category during index construction.

During bulk insertion, instead of immediately inserting each entry into the index structure, the system collects all item pointers for identical keys in these accumulator structures. This approach allows for more efficient index construction by:
- Reducing redundant key storage
- Enabling batch processing of item pointers
- Optimizing the final index structure layout

The structure uses a red-black tree for efficient insertion and lookup during the accumulation phase, with the tree organized by attribute number, key value, and null category.

## Parameters / Member Variables
- `rbtnode`: Red-black tree node structure for organizing accumulators in the tree
- `key`: The actual key value being accumulated (Datum type for flexibility)
- `category`: Categorizes the key type (normal key, null value, etc.) using GinNullCategory
- `attnum`: Attribute number (column index) this accumulator corresponds to
- `shouldSort`: Flag indicating whether the item pointer list needs sorting before final processing
- `list`: Dynamic array of ItemPointerData storing tuple identifiers for this key
- `maxcount`: Currently allocated size of the list array (for memory management)
- `count`: Number of valid entries currently stored in the list array

## Dependencies
- Functions called/Symbols referenced:
  - [RBTNode](../R/RBTNode.md) (red-black tree infrastructure)
  - GinNullCategory (key categorization)
  - [ItemPointerData](../I/ItemPointerData.md) (tuple identifier storage)
  - Datum (PostgreSQL's generic data type)

- Called from (representative examples):
  - ginCombineData (combines accumulators when duplicate keys found)
  - [cmpEntryAccumulator](../c/cmpEntryAccumulator.md) (compares accumulators for tree ordering)
  - [ginAllocEntryAccumulator](../g/ginAllocEntryAccumulator.md) (allocates new accumulator instances)
  - [ginInsertBAEntry](../g/ginInsertBAEntry.md) (inserts entries into build accumulator)
  - [ginGetBAEntry](../g/ginGetBAEntry.md) (retrieves entries from build accumulator)

## Notes and Other Information
- Used exclusively during GIN index construction via ginbulk.c routines
- Memory allocation is optimized using chunk-based allocation (DEF_NENTRY quantum)
- Item pointer lists start with DEF_NPTR (5) initial capacity and grow exponentially
- The shouldSort flag optimizes performance by tracking when item pointers become unordered
- Part of PostgreSQL's "fast build" mechanism for inverted indexes
- Memory usage is carefully tracked through the BuildAccumulator's allocatedMemory counter