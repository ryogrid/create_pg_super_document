# TypeCacheEnumData

## Location
src/backend/utils/cache/typcache.c: 138 - 144

## Overview
TypeCacheEnumData is a structure that caches information about enumerated types (enum types) to enable efficient comparison and ordering operations.

## Definition
```c
typedef struct TypeCacheEnumData
{
    Oid         bitmap_base;     /* OID corresponding to bit 0 of bitmapset */
    Bitmapset  *sorted_values;   /* Set of OIDs known to be in order */
    int         num_values;      /* total number of values in enum */
    EnumItem    enum_values[FLEXIBLE_ARRAY_MEMBER];
} TypeCacheEnumData;
```

## Detailed Description
TypeCacheEnumData is used by PostgreSQL's type cache system to store cached information about enumerated types. This structure enables efficient comparison operations between enum values by maintaining sorted order information and providing quick lookup capabilities. The cache tracks which enum values are known to be in their correct sort order through a bitmapset, allowing for optimized comparison operations without repeatedly querying the system catalogs.

The structure uses a flexible array member to store the actual enum items, making it memory-efficient by storing only the necessary number of enum values. The bitmap_base field establishes a reference point for the bitmapset, while sorted_values tracks which OIDs are confirmed to be in sorted order.

## Parameters / Member Variables
- `bitmap_base`: OID value that corresponds to bit 0 of the bitmapset, used as a reference point for tracking sorted values
- `sorted_values`: Bitmapset containing flags indicating which enum OIDs are known to be in their correct sort order
- `num_values`: Total count of values present in this enumerated type
- `enum_values[]`: Flexible array member containing the actual EnumItem structures for all enum values

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - Bitmapset (PostgreSQL bitmap set structure)
  - EnumItem (structure representing individual enum values)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member)
- Called from (representative examples):
  - enum_known_sorted
  - compare_values_of_enum
  - load_enum_cache_data
  - find_enumitem

## Notes and Other Information
- This structure is part of PostgreSQL's type cache system for optimizing enum type operations
- The bitmapset mechanism allows for efficient tracking of sort order without storing redundant information
- Located in src/backend/utils/cache/typcache.c as part of the type cache implementation
- The flexible array member design allows for efficient memory usage by allocating exactly the space needed for enum values
- Used primarily for optimizing enum comparison operations and maintaining sort order information