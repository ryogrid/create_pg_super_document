# ItemPointerGetOffsetNumber

## Location
src/include/storage/itemptr.h: 124 - 134

## Overview
Safely extracts the offset number from an ItemPointerData structure with validity checking enabled.

## Definition
static inline OffsetNumber ItemPointerGetOffsetNumber(const ItemPointerData *pointer)

## Detailed Description
ItemPointerGetOffsetNumber is the safe version of the offset number extraction function that includes validity checking. Before extracting the offset number, it uses an Assert to verify that the item pointer is valid via ItemPointerIsValid. If the assertion passes, it delegates to ItemPointerGetOffsetNumberNoCheck to perform the actual extraction. The offset number represents the position of a tuple within a specific page, which is crucial for precisely locating data within PostgreSQL's storage system.

This function provides the same safety-performance balance as its block number counterpart - assertions are compiled out in production builds while providing valuable debugging checks during development.

## Parameters / Member Variables
- pointer: A pointer to an ItemPointerData structure from which to extract the offset number

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerIsValid
  - ItemPointerGetOffsetNumberNoCheck
- Called from (representative examples):
  - heap_fetch
  - heap_insert
  - heap_delete
  - heap_update
  - brininsert
  - TidStoreIsMember
  - spgTestLeafTuple
  - ItemPointerEquals

## Notes and Other Information
- This is an inline function for performance optimization
- Uses Assert for validity checking, which is compiled out in production builds
- This is the checked version - recommended for general use over the NoCheck variant
- Extensively used throughout PostgreSQL for safe offset number access
- Returns an OffsetNumber type representing the tuple position within a page
- The Assert provides early detection of invalid pointer usage during development
- OffsetNumber values start from 1 for valid tuples, with 0 typically indicating invalid or special cases