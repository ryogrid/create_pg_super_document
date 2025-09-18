# record_fields_have_compare

## Location
src/backend/utils/cache/typcache.c: 1497 - 1504

## Overview
This function checks whether all fields of a record type support comparison operations, ensuring that record comparison operations can be performed safely.

## Definition
static bool record_fields_have_compare(TypeCacheEntry *typentry)

## Detailed Description
The function determines if a record type has comparison support by checking if all of its field types support comparison operations. It first ensures that the field properties have been cached by calling cache_record_field_properties if needed, then returns the cached result from the type cache entry flags. This is part of PostgreSQL's type system infrastructure that enables efficient determination of what operations are supported for composite types.

## Parameters / Member Variables
- typentry: Pointer to a TypeCacheEntry structure containing cached information about a record type, including flags indicating which operations are supported by the type's fields

## Dependencies
- Functions called/Symbols referenced:
  - [cache_record_field_properties](../c/cache_record_field_properties.md)
  - TCFLAGS_CHECKED_FIELD_PROPERTIES (flag)
  - TCFLAGS_HAVE_FIELD_COMPARE (flag)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md)

## Notes and Other Information
- This is a static function only used within typcache.c
- The function implements lazy evaluation - field properties are only computed when first needed
- The result is cached in the TypeCacheEntry flags to avoid repeated computation
- Essential for determining whether record types can participate in comparison operations like sorting and equality checks