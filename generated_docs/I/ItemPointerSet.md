# ItemPointerSet

## Location
src/include/storage/itemptr.h: 135 - 146

## Overview
Sets a disk item pointer to the specified block number and offset number, providing a convenient interface for initializing ItemPointerData structures with specific tuple location information.

## Definition


## Detailed Description
ItemPointerSet is a fundamental inline function in PostgreSQL's storage subsystem that initializes an ItemPointerData structure with a specific block number and offset number. The function serves as the primary method for setting tuple location information within the database's physical storage layout. It combines block identification and position within that block into a single atomic operation, ensuring consistency when establishing references to specific tuples on disk.

The function performs validation through assertions and delegates the actual block ID setting to BlockIdSet while directly assigning the offset number. This design maintains the separation of concerns between block-level and intra-block positioning while providing a unified interface for complete item pointer initialization.

## Parameters / Member Variables
- : Pointer to the ItemPointerData structure to be initialized (must be valid)
- : The block number within the relation where the tuple is located
- : The offset number within the block identifying the specific tuple position

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (assertion validation)
  - BlockIdSet (sets the block identifier portion)
- Called from (representative examples):
  - brin_doupdate
  - heapgettup
  - RelationPutHeapTuple
  - ItemPointerInc
  - TidRangeEval

## Notes and Other Information
- This is an inline function defined in itemptr.h for optimal performance
- Includes assertion checking to ensure pointer validity before use
- Fundamental building block for tuple identification throughout PostgreSQL
- Used extensively in heap access methods, index operations, and tuple manipulation
- Part of the core storage abstraction layer that enables PostgreSQL's MVCC implementation