# record_fields_have_hashing

## Location
[src/backend/utils/cache/typcache.c:1505-1512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1505-L1512)

## Overview
This function checks whether all fields of a record type support hashing operations, enabling hash-based operations like hash joins and hash aggregation on record types.

## Definition
static bool record_fields_have_hashing(TypeCacheEntry *typentry)

## Detailed Description
The function determines if a record type has hashing support by checking if all of its field types support hash operations. It follows the same lazy evaluation pattern as other field property checking functions, ensuring that field properties are cached via cache_record_field_properties before returning the cached result from the type cache entry flags. This enables PostgreSQL to determine whether record types can be used in hash-based operations.

## Parameters / Member Variables
- typentry: Pointer to a TypeCacheEntry structure containing cached information about a record type, including flags indicating which operations are supported by the type's fields

## Dependencies
- Functions called/Symbols referenced:
  - [cache_record_field_properties](../c/cache_record_field_properties.md)
  - TCFLAGS_CHECKED_FIELD_PROPERTIES (flag)
  - TCFLAGS_HAVE_FIELD_HASHING (flag)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md)

## Notes and Other Information
- This is a static function only used within typcache.c
- Uses lazy evaluation pattern - field properties are computed only when first needed
- The result is cached to avoid repeated computation of field properties
- Critical for enabling hash-based operations like hash joins, hash aggregation, and hash partitioning on record types
- Works in conjunction with record_fields_have_compare to provide complete operation support information