# ItemPointerSetInvalid

## Location
[src/include/storage/itemptr.h:184-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L184-L196)

## Overview
Sets a disk item pointer to an invalid state by assigning invalid values to both the block number and offset number components, marking the pointer as unusable.

## Definition

```c
static inline void
ItemPointerSetInvalid(ItemPointerData *pointer)
```
## Detailed Description
ItemPointerSetInvalid initializes an ItemPointerData structure to represent an invalid or null tuple reference. This function is essential for indicating that an item pointer does not reference any valid tuple location within the database. It sets both the block ID to InvalidBlockNumber and the position ID to InvalidOffsetNumber, creating a clearly identifiable invalid state that can be tested by other functions.

This function is commonly used during tuple deletion, cleanup operations, error handling, and initialization scenarios where a definitive invalid state is required. The invalid state is consistent and recognizable throughout the PostgreSQL codebase, making it safe for comparisons and conditional logic.

## Parameters / Member Variables
- : Pointer to the ItemPointerData structure to be invalidated (must be valid)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (assertion validation)
  - [BlockIdSet](../B/BlockIdSet.md) (sets block ID to InvalidBlockNumber)
  - InvalidOffsetNumber (constant for invalid offset)
- Called from (representative examples):
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [entryGetItem](../e/entryGetItem.md)
  - [toast_flatten_tuple_to_datum](../t/toast_flatten_tuple_to_datum.md)
  - AfterTriggerSaveEvent
  - [ExecCheckIndexConstraints](../E/ExecCheckIndexConstraints.md)

## Notes and Other Information
- This is an inline function defined in itemptr.h for optimal performance
- Creates a consistent invalid state recognizable throughout PostgreSQL
- Used extensively in tuple manipulation, cleanup, and error handling
- Essential for proper initialization of ItemPointer structures
- The invalid state can be tested using ItemPointerIsValid()
- Commonly used when clearing tuple slot references and during vacuum operations
- Important for maintaining data integrity during transaction processing