# ItemPointerGetBlockNumberNoCheck

## Location
src/include/storage/itemptr.h: 93 - 102

## Overview
Extracts the block number from an ItemPointerData structure without performing validity checks.

## Definition
static inline BlockNumber ItemPointerGetBlockNumberNoCheck(const ItemPointerData *pointer)

## Detailed Description
ItemPointerGetBlockNumberNoCheck is a low-level utility function that directly extracts the block number component from an ItemPointerData structure. Unlike its checked counterpart, this function does not validate whether the pointer is valid before accessing its contents, making it faster but potentially unsafe if used with invalid pointers. The function delegates to BlockIdGetBlockNumber to extract the block number from the ip_blkid field of the item pointer.

This function is primarily used in performance-critical code paths where the validity of the pointer has already been established, or in utility functions that need to access block numbers regardless of pointer validity.

## Parameters / Member Variables
- pointer: A pointer to an ItemPointerData structure from which to extract the block number

## Dependencies
- Functions called/Symbols referenced:
  - BlockIdGetBlockNumber
- Called from (representative examples):
  - heap_set_tidrange
  - ItemPointerCompare
  - ItemPointerInc
  - ItemPointerDec
  - ItemPointerGetBlockNumber
  - GinItemPointerGetBlockNumber

## Notes and Other Information
- This is an inline function for performance optimization
- No validity checking is performed - callers must ensure the pointer is valid
- Used extensively in item pointer comparison and manipulation functions
- The NoCheck suffix indicates this is the unchecked version of the function
- Returns a BlockNumber type representing the physical block number on disk