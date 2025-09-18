# ItemPointerCopy

## Location
src/include/storage/itemptr.h: 172 - 183

## Overview
Copies the complete contents of one disk item pointer to another, providing a safe and validated method for duplicating ItemPointerData structures.

## Definition


## Detailed Description
ItemPointerCopy performs a complete bitwise copy of an ItemPointerData structure from a source to a destination location. This function is crucial for operations that need to duplicate tuple references, such as maintaining backup copies during updates, creating multiple references to the same tuple, or transferring item pointer information between different data structures.

The function uses simple structure assignment (*toPointer = *fromPointer) which copies both the block ID and offset number components atomically. The implementation includes validation for both source and destination pointers and explicitly notes that padding considerations may be important since ItemPointer structures are used as hash keys in various contexts.

## Parameters / Member Variables
- : Source ItemPointerData structure to copy from (must be valid, const)
- : Destination ItemPointerData structure to copy to (must be valid)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (assertion validation for both parameters)
- Called from (representative examples):
  - heap_lock_tuple
  - heapam_tuple_insert
  - _bt_form_posting
  - AfterTriggerSaveEvent
  - TidRangeEval

## Notes and Other Information
- This is an inline function defined in itemptr.h for optimal performance
- Uses bitwise structure copy which is safe for ItemPointerData
- Important note about potential padding concerns when used as hash keys
- Validates both source and destination pointers before copying
- Commonly used in tuple manipulation, index operations, and trigger processing
- Essential for maintaining tuple reference integrity during complex operations
- Performs atomic copy of both block number and offset number components