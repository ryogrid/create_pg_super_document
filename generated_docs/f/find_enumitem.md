# find_enumitem

## Location
src/backend/utils/cache/typcache.c: 2705 - 2721

## Overview
Locates an EnumItem with a given OID within the enum type's cached data structure using binary search.

## Definition


## Detailed Description
The  function performs a binary search to locate a specific enum value within the cached enum data structure. It searches through the  array in the  structure to find an  that matches the provided OID. The function uses the  comparison function to perform the binary search via the standard C library's  function. The implementation includes a safety check for empty arrays to prevent core dumps on certain Solaris versions.

## Parameters / Member Variables
- : Pointer to the TypeCacheEnumData structure containing cached enum information
- : The OID of the enum value to search for

## Dependencies
- Functions called/Symbols referenced:
  - bsearch (C standard library function)
  - enum_oid_cmp (comparison function for OID ordering)
- Data structures used:
  - TypeCacheEnumData
  - EnumItem
- Called from (representative examples):
  - compare_values_of_enum (multiple calls for enum value comparison)

## Notes and Other Information
- Returns NULL if the enum value is not found or if the enum data contains no values
- The function is static and only used within the typcache.c module
- Includes a specific workaround for Solaris systems where bsearch on zero items could cause crashes
- The enum_values array must be sorted by OID for binary search to work correctly
- Used primarily for enum value comparison operations in the type cache system