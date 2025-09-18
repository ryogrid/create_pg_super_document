# bms_free

## Location
src/backend/nodes/bitmapset.c: 239 - 250

## Overview
Safely deallocates memory used by a Bitmapset, with NULL pointer protection.

## Definition


## Detailed Description
This function frees the memory allocated for a Bitmapset structure. It provides a safe wrapper around PostgreSQL's pfree() function by first checking if the pointer is non-NULL before attempting to free it. This prevents crashes that would occur if pfree() were called directly on a NULL pointer.

## Parameters / Member Variables
- : Pointer to the Bitmapset to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - pfree (PostgreSQL memory deallocation function)

- Called from (representative examples):
  - heap_update
  - bms_copy_and_free
  - check_index_only
  - try_nestloop_path
  - extract_rollup_sets
  - reduce_outer_joins_pass2
  - RelationDestroyRelation
  - RelationGetIndexAttrBitmap

## Notes and Other Information
- Unlike standard pfree(), this function safely handles NULL input pointers
- Used extensively throughout PostgreSQL for cleanup of temporary bitmapsets
- Essential for preventing memory leaks when working with dynamically allocated bitmapsets
- The function follows PostgreSQL's naming convention for bitmapset operations with the 'bms_' prefix
- Commonly used in query optimization, join processing, and relation management code