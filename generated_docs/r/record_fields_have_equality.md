# record_fields_have_equality

## Location
[src/backend/utils/cache/typcache.c:1489-1496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1489-L1496)

## Overview
Determines whether all fields in a composite/record type have equality operators available.

## Definition
```c
static bool record_fields_have_equality(TypeCacheEntry *typentry)
```

## Detailed Description
This function checks if all fields of a composite (record) type have equality operators available. It follows the same lazy evaluation pattern as the array element property functions, first ensuring that field properties have been cached by checking the `TCFLAGS_CHECKED_FIELD_PROPERTIES` flag, and if not, it calls `cache_record_field_properties()` to populate the cache. Once the properties are cached, it returns whether the `TCFLAGS_HAVE_FIELD_EQUALITY` flag is set, indicating that equality operations are available for all fields in the record type.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry structure containing cached type information for the composite/record type

## Dependencies
- Functions called/Symbols referenced:
  - [cache_record_field_properties](../c/cache_record_field_properties.md)
  - TCFLAGS_CHECKED_FIELD_PROPERTIES (flag constant)
  - TCFLAGS_HAVE_FIELD_EQUALITY (flag constant)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (at line 579)

## Notes and Other Information
This is a static function in typcache.c that serves as part of the type caching system for composite types, complementing the array element property functions. The function is part of a family of helper functions for composite types, as indicated by the comment "Likewise, some helper functions for composite types." It implements the same lazy evaluation pattern to avoid unnecessary computation. Equality testing for composite types requires that all constituent fields support equality operations, which makes this check essential for determining whether record/row comparisons can be performed in PostgreSQL operations.

## Simplified Source

```c
// Simplified version of record_fields_have_equality
static bool record_fields_have_equality(TypeCacheEntry *typentry) {
    // Check if field properties have been cached yet
    if (!(typentry->flags & TCFLAGS_CHECKED_FIELD_PROPERTIES)) {
        // Cache the field properties if not done yet
        cache_record_field_properties(typentry);
    }

    // Return whether all fields support equality operations
    return (typentry->flags & TCFLAGS_HAVE_FIELD_EQUALITY) != 0;
}
```

Key simplifications made:
- Added clear comments explaining the lazy evaluation pattern
- Clarified what each flag check accomplishes
- Maintained the essential logic flow for record field equality support