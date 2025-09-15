# record_fields_have_extended_hashing

## Overview
The record_fields_have_extended_hashing function determines whether all fields in a composite record type support extended hash operations (64-bit hash functions) by checking cached field properties in PostgreSQL's type cache system. This function provides optimized access to extended hash capability information for composite types, enabling advanced query optimization decisions for hash-based operations that require enhanced hash distribution. It serves as a specialized component in PostgreSQL's extended hashing infrastructure where 64-bit hash functions are preferred for improved collision resistance and distribution quality.

## Definition
```c
static bool
record_fields_have_extended_hashing(TypeCacheEntry *typentry)
{
    if (!(typentry->flags & TCFLAGS_CHECKED_FIELD_PROPERTIES))
        cache_record_field_properties(typentry);
    return (typentry->flags & TCFLAGS_HAVE_FIELD_EXTENDED_HASHING) != 0;
}
```

## Detailed Description
record_fields_have_extended_hashing implements PostgreSQL's sophisticated extended hashing capability verification system within the type cache infrastructure, following the established lazy evaluation pattern for optimal performance. The function ensures field properties are analyzed and cached by checking TCFLAGS_CHECKED_FIELD_PROPERTIES and invoking cache_record_field_properties() if necessary, then returns the cached extended hashing capability from the TCFLAGS_HAVE_FIELD_EXTENDED_HASHING flag. Extended hashing support indicates that all component fields provide 64-bit hash functions rather than traditional 32-bit variants, offering superior hash distribution characteristics and reduced collision rates for large datasets. This capability is crucial for PostgreSQL's advanced hash-based operations including high-cardinality hash joins, large-scale hash aggregations, and hash partitioning scenarios where enhanced hash quality directly impacts performance. The function enables the query planner to select optimal algorithms based on extended hash availability, potentially choosing more sophisticated hashing strategies when supported by all composite type fields.

## Parameters / Member Variables
- `typentry`: Pointer to a TypeCacheEntry structure representing the composite type being evaluated, containing comprehensive cached metadata including field definitions, extended hash function availability flags, and property analysis completion status

## Dependencies
- **Functions called/Symbols referenced**:
  - `cache_record_field_properties` - Performs comprehensive analysis and caching of field properties including extended hash function availability
  - `TCFLAGS_CHECKED_FIELD_PROPERTIES` - Flag constant indicating completion of field property analysis
  - `TCFLAGS_HAVE_FIELD_EXTENDED_HASHING` - Flag constant indicating that all fields support extended (64-bit) hash operations
- **Called from (representative examples)**:
  - `lookup_type_cache` - Central type cache function that uses this to determine extended hash capabilities for requested type features
  - Advanced hash join planning code - Invoked during query planning to validate extended hash join feasibility for composite types with high cardinality
  - Hash partitioning handlers - Used to verify extended hash function support for improved partition distribution

## Notes & Other Information
This function represents PostgreSQL's commitment to advanced hash-based optimization techniques, where extended hashing provides measurable performance improvements for large-scale operations through better hash distribution and reduced collision rates. The lazy evaluation design ensures computational efficiency by caching expensive field analysis results while maintaining accurate capability information across query executions. Extended hashing support is particularly valuable in data warehousing scenarios and analytical workloads where composite types with many distinct values benefit from enhanced hash quality. The function operates within PostgreSQL's backend-local type cache system, ensuring thread safety while avoiding synchronization overhead. Cache invalidation is handled at the type cache level, maintaining consistency when type definitions change, though such changes are rare within active sessions.