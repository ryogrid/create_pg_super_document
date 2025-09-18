# ItemPointerGetBlockNumber

## Location
src/include/storage/itemptr.h: 103 - 113

## Overview
Safely extracts the block number from an ItemPointerData structure with validity checking enabled.

## Definition
static inline BlockNumber ItemPointerGetBlockNumber(const ItemPointerData *pointer)

## Detailed Description
ItemPointerGetBlockNumber is the safe version of the block number extraction function that includes validity checking. Before extracting the block number, it uses an Assert to verify that the item pointer is valid via ItemPointerIsValid. If the assertion passes, it delegates to ItemPointerGetBlockNumberNoCheck to perform the actual extraction. This function provides a balance between safety and performance - it includes debugging checks in debug builds while maintaining efficiency in production builds through the use of Assert rather than runtime error handling.

This is the recommended function to use when extracting block numbers from item pointers in most situations, as it provides safety guarantees while maintaining good performance.

## Parameters / Member Variables
- pointer: A pointer to an ItemPointerData structure from which to extract the block number

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerIsValid
  - ItemPointerGetBlockNumberNoCheck
- Called from (representative examples):
  - heap_fetch
  - heap_insert
  - heap_delete
  - heap_update
  - brininsert
  - TidStoreIsMember
  - gistdoinsert

## Notes and Other Information
- This is an inline function for performance optimization
- Uses Assert for validity checking, which is compiled out in production builds
- This is the checked version - recommended for general use
- Widely used throughout PostgreSQL core for safe block number access
- Returns a BlockNumber type representing the physical block number on disk
- The Assert ensures that invalid pointers are caught during development and testing