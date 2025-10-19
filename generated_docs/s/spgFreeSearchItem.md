# spgFreeSearchItem

## Location
[src/backend/access/spgist/spgscan.c:84-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L84-L107)

## Overview
A cleanup function that properly deallocates memory for a SpGistSearchItem structure and all its associated dynamically allocated components.

## Definition

```c
static void
spgFreeSearchItem(SpGistScanOpaque so, SpGistSearchItem *item)
```
## Detailed Description
This function handles the complete deallocation of a SpGistSearchItem structure, taking care to properly free all dynamically allocated memory components. The function implements type-aware memory management, distinguishing between leaf and inner node items to determine the correct data type for the value field. It safely handles potentially NULL pointers and ensures no memory leaks occur during SP-GiST search operations.

The function follows PostgreSQL's memory management conventions, using pfree() for all deallocations and checking for NULL pointers before freeing. The value field requires special handling because its type depends on whether the item represents a leaf or inner node.

## Parameters / Member Variables
- `so`: SpGistScanOpaque structure containing the scan operation context and type information needed for proper memory management
- `*item`: Pointer to the SpGistSearchItem structure to be freed, along with all its allocated components
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (PostgreSQL utility to extract pointer from Datum)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - SpGistScanOpaque (scan context structure type)
  - [SpGistSearchItem](../S/SpGistSearchItem.md) (search item structure type)
- Called from (representative examples):
  - [spgWalk](spgWalk.md) (main search traversal function that cleans up processed items)

## Notes and Other Information
- The value field type depends on whether the item is a leaf node (attType) or inner node (attLeafType) - note the confusing but intentional reversal mentioned in comments
- Only frees value pointer if the corresponding data type is not pass-by-value (attbyval check)
- Safely handles NULL pointers for leafTuple and traversalValue fields
- Part of the memory management infrastructure for SP-GiST search operations
- Essential for preventing memory leaks during complex search tree traversals

## Simplified Source

```c
static void spgFreeSearchItem(SpGistScanOpaque so, SpGistSearchItem *item) {
    // Free the value field if it's not pass-by-value and not NULL
    // Note: Type selection is reversed (leaf uses attType, inner uses attLeafType)
    bool valueIsPassByValue = item->isLeaf ?
        so->state.attType.attbyval :
        so->state.attLeafType.attbyval;

    if (!valueIsPassByValue && DatumGetPointer(item->value) != NULL) {
        pfree(DatumGetPointer(item->value));
    }

    // Free optional fields if they exist
    if (item->leafTuple)
        pfree(item->leafTuple);

    if (item->traversalValue)
        pfree(item->traversalValue);

    // Free the item structure itself
    pfree(item);
}
```