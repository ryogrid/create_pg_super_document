# BlockIdEquals

## Location
src/include/storage/block.h: 92 - 102

## Overview
BlockIdEquals is an inline function that compares two block identifiers for equality by checking if both their high and low 16-bit components match.

## Definition
```c
static inline bool BlockIdEquals(const BlockIdData *blockId1, const BlockIdData *blockId2)
```

## Detailed Description
BlockIdEquals performs a bitwise comparison of two BlockIdData structures to determine if they represent the same block number. The function checks equality by comparing both the bi_hi (high 16 bits) and bi_lo (low 16 bits) fields of the two block identifiers. This provides an efficient way to test block identifier equality without reconstructing the full 32-bit block numbers, making it suitable for performance-critical operations in PostgreSQL's storage management.

## Parameters / Member Variables
- `blockId1`: Pointer to the first BlockIdData structure to compare (const-qualified)
- `blockId2`: Pointer to the second BlockIdData structure to compare (const-qualified)

## Dependencies
- Functions called/Symbols referenced:
  - BlockIdData (structure type)
- Called from (representative examples):
  - Currently no direct callers found in the codebase (may be used via macros or inlined)

## Notes and Other Information
- This is a static inline function defined in src/include/storage/block.h for optimal performance
- Returns true if both bi_hi and bi_lo fields match between the two block identifiers
- The function uses const-qualified parameters indicating it does not modify the input structures
- Part of PostgreSQL's block management API for efficient block identifier operations
- Provides a clean abstraction for block identifier comparison without exposing internal bit manipulation details