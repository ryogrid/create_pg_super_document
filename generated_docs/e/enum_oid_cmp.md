# enum_oid_cmp

## Location
[src/backend/utils/cache/typcache.c:2722-2734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2722-L2734)

## Overview
A comparison function used for sorting and searching EnumItem structures by their OID values in ascending order.

## Definition

```c
static int
enum_oid_cmp(const void *left, const void *right)
```
## Detailed Description
The  function is a standard comparison function designed for use with sorting and searching algorithms like qsort and bsearch. It compares two EnumItem structures based on their enum_oid fields using PostgreSQL's  utility function. The function follows the standard C library comparison convention, returning a negative value if the left OID is smaller, zero if they are equal, and a positive value if the left OID is larger.

## Parameters / Member Variables
- : Pointer to the first EnumItem structure to compare (cast from void*)
- : Pointer to the second EnumItem structure to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_u32](../p/pg_cmp_u32.md) (PostgreSQL utility function for comparing unsigned 32-bit values)
- Data structures used:
  - [EnumItem](../E/EnumItem.md)
- Called from (representative examples):
  - [load_enum_cache_data](../l/load_enum_cache_data.md) (for sorting enum values during cache loading)
  - [find_enumitem](../f/find_enumitem.md) (for binary search operations)

## Notes and Other Information
- Returns standard comparison result: negative, zero, or positive integer
- The function is static and only used within the typcache.c module
- Essential for maintaining sorted order of enum values in the type cache
- Enables efficient binary search operations on enum value arrays
- Uses PostgreSQL's type-safe comparison utilities rather than direct arithmetic