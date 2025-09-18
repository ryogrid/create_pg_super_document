# cache_record_field_properties

## Location
src/backend/utils/cache/typcache.c: 1521 - 1625

## Overview
This function computes and caches the field properties for record types, determining which operations (equality, comparison, hashing, extended hashing) are supported by all fields of a composite type.

## Definition
static void cache_record_field_properties(TypeCacheEntry *typentry)

## Detailed Description
This function is the core implementation for determining what operations are supported by record types. It handles three main cases: RECORD pseudo-type (assumes equality and comparison), composite types (checks all fields), and domains over composite types (inherits base type properties). For composite types, it iterates through all non-dropped fields and checks if each field type supports the required operations. Only if ALL fields support an operation is that operation marked as available for the record type. The function uses reference counting for tuple descriptors to ensure safe access during catalog lookups.

## Parameters / Member Variables
- typentry: Pointer to a TypeCacheEntry structure that will be updated with computed field property flags indicating which operations are supported by the record type

## Dependencies
- Functions called/Symbols referenced:
  - load_typcache_tupdesc
  - IncrTupleDescRefCount
  - DecrTupleDescRefCount
  - lookup_type_cache
  - getBaseTypeAndTypmod
  - TCFLAGS_HAVE_FIELD_EQUALITY (flag)
  - TCFLAGS_HAVE_FIELD_COMPARE (flag)
  - TCFLAGS_HAVE_FIELD_HASHING (flag)
  - TCFLAGS_HAVE_FIELD_EXTENDED_HASHING (flag)
  - TCFLAGS_CHECKED_FIELD_PROPERTIES (flag)
  - TCFLAGS_DOMAIN_BASE_IS_COMPOSITE (flag)
- Called from (representative examples):
  - record_fields_have_equality
  - record_fields_have_compare
  - record_fields_have_hashing
  - record_fields_have_extended_hashing

## Notes and Other Information
- This is a static function only used within typcache.c
- Implements careful tuple descriptor reference counting to prevent crashes during catalog lookups
- For RECORD pseudo-type, conservatively assumes equality and comparison work but not hashing
- Uses early exit optimization - stops checking fields once all property flags are cleared
- For domain types over composite types, inherits properties from the base composite type
- Sets TCFLAGS_CHECKED_FIELD_PROPERTIES to prevent redundant computation
- Critical for PostgreSQL's type system to determine what operations can be performed on complex types