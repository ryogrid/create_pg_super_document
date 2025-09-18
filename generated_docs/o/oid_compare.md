# oid_compare

## Location
src/backend/utils/cache/syscache.c: 796 - 802

## Overview
A static comparison function used by qsort to compare Object Identifier (OID) values in ascending order for sorting operations in PostgreSQL's system cache.

## Definition
```c
static int oid_compare(const void *a, const void *b)
```

## Detailed Description
The `oid_compare` function is a comparator function specifically designed for use with the standard C library's `qsort` function. It compares two OID (Object Identifier) values to determine their relative ordering. The function follows the standard qsort comparator contract by returning a negative value if the first OID is less than the second, zero if they are equal, and a positive value if the first OID is greater than the second. This function is used internally within PostgreSQL's system cache implementation to maintain sorted arrays of OIDs for efficient lookup operations.

## Parameters / Member Variables
- `a`: Pointer to the first OID value to compare (cast from const void* to const Oid*)
- `b`: Pointer to the second OID value to compare (cast from const void* to const Oid*)

## Dependencies
- Functions called/Symbols referenced:
  - pg_cmp_u32
- Called from (representative examples):
  - InitCatalogCache (multiple times at lines 153, 156, 159, 162)
  - Used in KEY macro definition at line 98

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the syscache.c compilation unit
- The function uses PostgreSQL's pg_cmp_u32 utility function to perform the actual unsigned 32-bit integer comparison
- As a qsort comparator, this function enables efficient sorting of OID arrays, which is crucial for system cache performance
- The function is referenced multiple times in InitCatalogCache, indicating its importance in setting up various system caches during PostgreSQL initialization