# record_fields_have_hashing

## Overview
The record_fields_have_hashing function determines whether all fields in a composite record type support hash operations by checking cached field properties in PostgreSQL's type cache system. This function provides optimized access to hash capability information for composite types, enabling efficient query planning decisions for hash-based operations. It serves as a critical component in PostgreSQL's hash join and hash aggregation optimization pathways where hash function availability must be verified before algorithm selection.

## Definition
```c
static bool
record_fields_have_hashing(TypeCacheEntry *typentry)
{
    if (!(typentry->flags & TCFLAGS_CHECKED_FIELD_PROPERTIES))
        cache_record_field_properties(typentry);
    return (typentry->flags & TCFLAGS_HAVE_FIELD_HASHING) != 0;
}
```

## Detailed Description
record_fields_have_hashing implements PostgreSQL's lazy evaluation strategy for composite type hash capability verification within the sophisticated type cache infrastructure. The function follows a two-phase approach: first ensuring that field properties have been analyzed and cached by checking the TCFLAGS_CHECKED_FIELD_PROPERTIES flag and invoking cache_record_field_properties() if necessary, then returning the cached hash capability result from the TCFLAGS_HAVE_FIELD_HASHING flag. This design pattern minimizes computational overhead by avoiding redundant field analysis while guaranteeing accurate hash function availability information for query optimization decisions. The function is particularly important for hash-based algorithms in PostgreSQL's executor, where hash function availability determines whether efficient hash joins, hash aggregations, or hash-based unique operations can be employed. Performance optimization is achieved through single-computation caching that amortizes expensive field analysis across multiple queries.

## Parameters / Member Variables
- `typentry`: Pointer to a TypeCacheEntry structure representing the composite type being evaluated, containing comprehensive cached metadata including field definitions, hash function availability flags, and property analysis completion markers

## Dependencies
- **Functions called/Symbols referenced**:
  - `cache_record_field_properties` - Performs comprehensive analysis and caching of field properties including hash function availability
  - `TCFLAGS_CHECKED_FIELD_PROPERTIES` - Flag constant indicating completion of field property analysis
  - `TCFLAGS_HAVE_FIELD_HASHING` - Flag constant indicating that all fields support hash operations
- **Called from (representative examples)**:
  - `lookup_type_cache` - Central type cache function that uses this to determine hash capabilities for requested type features
  - Hash join planning code - Invoked during query planning to validate hash join feasibility for composite types
  - Hash aggregation handlers - Used to verify hash function support before selecting hash-based aggregation algorithms

## Notes & Other Information
This function is integral to PostgreSQL's high-performance query execution system, where hash-based algorithms provide significant performance advantages over sort-based alternatives when hash functions are available. The lazy caching approach ensures optimal resource utilization by computing field properties only when needed, while maintaining cache coherence across query executions. The function operates within PostgreSQL's backend-local type cache architecture, ensuring thread safety without requiring explicit locking mechanisms. Hash capability verification is essential for maintaining query correctness, as attempting hash operations on types without proper hash functions would produce incorrect results. The cached results remain valid for the duration of the backend session, reflecting the immutable nature of type definitions within PostgreSQL's type system.