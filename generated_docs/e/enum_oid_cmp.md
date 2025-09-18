# enum_oid_cmp

## Location
src/backend/utils/cache/typcache.c: 2722 - 2734

## Overview
A comparison function used for sorting and searching EnumItem structures by their OID values in ascending order.

## Definition


## Detailed Description
The  function is a standard comparison function designed for use with sorting and searching algorithms like qsort and bsearch. It compares two EnumItem structures based on their enum_oid fields using PostgreSQL's  utility function. The function follows the standard C library comparison convention, returning a negative value if the left OID is smaller, zero if they are equal, and a positive value if the left OID is larger.

## Parameters / Member Variables
- : Pointer to the first EnumItem structure to compare (cast from void*)
- : Pointer to the second EnumItem structure to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - pg_cmp_u32 (PostgreSQL utility function for comparing unsigned 32-bit values)
- Data structures used:
  - EnumItem
- Called from (representative examples):
  - load_enum_cache_data (for sorting enum values during cache loading)
  - find_enumitem (for binary search operations)

## Notes and Other Information
- Returns standard comparison result: negative, zero, or positive integer
- The function is static and only used within the typcache.c module
- Essential for maintaining sorted order of enum values in the type cache
- Enables efficient binary search operations on enum value arrays
- Uses PostgreSQL's type-safe comparison utilities rather than direct arithmetic