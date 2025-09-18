# bitno_to_blkno

## Location
src/backend/access/hash/hashovfl.c: 35 - 61

## Overview
Converts an overflow page bit number (its index in the free-page bitmaps) to the corresponding block number within the hash index.

## Definition


## Detailed Description
This function performs a critical conversion in PostgreSQL's hash index overflow page management system. It takes a zero-based bit number from the overflow page bitmap and converts it to an absolute block number within the hash index file. The conversion process involves:

1. Converting the zero-based bit number to a 1-based page number
2. Determining which split level this overflow page belongs to by examining the spares array
3. Calculating the absolute block number by adding the total number of bucket pages that exist before the identified split point

The function is essential for translating between the logical bitmap representation of free overflow pages and their physical storage locations in the index file.

## Parameters / Member Variables
- : Pointer to the hash index metadata page containing split point information and spares array
- : Zero-based bit number representing the overflow page's position in the bitmap

## Dependencies
- Functions called/Symbols referenced:
  - HashMetaPage (metadata structure)
  - _hash_get_totalbuckets (calculates total bucket count for a split level)
- Called from (representative examples):
  - [_hash_addovflpage](../h/_hash_addovflpage.md) (when adding new overflow pages)

## Notes and Other Information
- This is a static function, only accessible within the hashovfl.c module
- The conversion relies on PostgreSQL's hash index splitting mechanism where buckets are split incrementally
- The spares array in the metadata page tracks the number of overflow pages allocated at each split level
- The function assumes the ovflbitnum parameter is valid and within the current range of allocated overflow pages