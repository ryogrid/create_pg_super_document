# record_fields_have_extended_hashing

## Location
src/backend/utils/cache/typcache.c: 1513 - 1520

## Overview
This function checks whether all fields of a record type support extended hashing operations, which are required for advanced hash-based operations with improved collision resistance.

## Definition
static bool record_fields_have_extended_hashing(TypeCacheEntry *typentry)

## Detailed Description
The function determines if a record type has extended hashing support by checking if all of its field types support extended hash operations. Extended hashing provides better hash distribution and collision resistance compared to basic hashing. Like other field property checking functions, it uses lazy evaluation by ensuring field properties are cached through cache_record_field_properties before returning the cached result from the type cache entry flags.

## Parameters / Member Variables
- typentry: Pointer to a TypeCacheEntry structure containing cached information about a record type, including flags indicating which operations are supported by the type's fields

## Dependencies
- Functions called/Symbols referenced:
  - cache_record_field_properties
  - TCFLAGS_CHECKED_FIELD_PROPERTIES (flag)
  - TCFLAGS_HAVE_FIELD_EXTENDED_HASHING (flag)
- Called from (representative examples):
  - lookup_type_cache

## Notes and Other Information
- This is a static function only used within typcache.c
- Implements lazy evaluation pattern consistent with other field property checkers
- Extended hashing provides improved hash distribution compared to basic hashing
- The result is cached to avoid recomputation of field properties
- Used to determine eligibility for advanced hash-based operations that require better collision resistance
- Part of PostgreSQL's tiered hashing system where extended hashing is an enhancement over basic hashing