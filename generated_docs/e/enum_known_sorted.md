# enum_known_sorted

## Location
src/backend/utils/cache/typcache.c: 2448 - 2476

## Overview
enum_known_sorted is a static inline helper function that checks whether a given enum value OID is part of the subset that can be sorted by direct OID comparisons rather than requiring explicit enum value lookups.

## Definition
static inline bool enum_known_sorted(TypeCacheEnumData *enumdata, Oid arg)

## Detailed Description
This function determines if a given enum value OID falls within the range of values that are known to be sortable by simple OID comparison. It uses a bitmap-based approach to efficiently track which enum values maintain their sort order when compared by OID value rather than by their logical enum ordering.

The function performs bounds checking to ensure the OID falls within the cached range (above bitmap_base and within INT_MAX offset), then checks the corresponding bit in the sorted_values bitmap to determine if this particular enum value can be safely compared by OID.

This optimization is crucial for enum comparison performance, as it allows the system to use fast OID comparisons for known-sorted values rather than expensive catalog lookups for every comparison.

## Parameters / Member Variables
- `enumdata`: Pointer to TypeCacheEnumData structure containing cached enum information and the sorted values bitmap
- `arg`: The enum value OID to check for sortability

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_member
  - TypeCacheEnumData
- Called from (representative examples):
  - compare_values_of_enum

## Notes and Other Information
- This is a static inline function for performance, only accessible within typcache.c
- Uses bitmap-based tracking for efficient membership testing
- Performs bounds checking to prevent integer overflow
- Part of PostgreSQL's enum type optimization system for faster comparisons
- The bitmap_base represents the lowest OID in the sortable range
- Returns false for OIDs outside the trackable range or not in the sorted subset