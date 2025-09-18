# CatCacheFreeKeys

## Location
src/backend/utils/cache/catcache.c: 2261 - 2285

## Overview
Helper function that frees dynamically allocated memory for cache keys stored in the keys array, handling by-reference data types properly.

## Definition


## Detailed Description
CatCacheFreeKeys is a utility function that properly deallocates memory for cache key values that are stored by reference (not by value). It iterates through each key in the provided array and checks the corresponding attribute's storage characteristics using the tuple descriptor. For attributes that are stored by reference (attbyval = false), it calls pfree() to deallocate the memory pointed to by the Datum.

This function is essential for preventing memory leaks when removing cache entries, as by-reference keys typically point to separately allocated memory that must be explicitly freed. By-value keys (like integers) don't require special cleanup since their values are stored directly in the Datum.

## Parameters
- : Tuple descriptor containing attribute information for the cached relation
- : Number of keys to process
- : Array of attribute numbers corresponding to the cache keys
- : Array of Datum values representing the cache keys to be freed

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro for accessing attribute descriptors)
  - DatumGetPointer (macro for converting Datum to pointer)
  - pfree (PostgreSQL memory deallocation function)
  - Assert (for validation checks)
- Called from (representative examples):
  - CatCacheRemoveCTup
  - CatCacheRemoveCList

## Notes and Other Information
- This is a static function, only callable from within catcache.c
- Only frees memory for by-reference data types (attbyval = false)
- Includes assertion to ensure system attributes are not used (attnum > 0)
- Essential for proper memory management when removing cache entries
- Part of PostgreSQL's catalog cache cleanup infrastructure
- Handles the complexity of PostgreSQL's varied data type storage methods
- Must be called before deallocating the keys array itself