# brin_inclusion_opcinfo

## Location
src/backend/access/brin/brin_inclusion.c: 94 - 137

## Overview
BRIN inclusion operator class information function that initializes and returns metadata required for inclusion-based BRIN indexing operations on a specific data type.

## Definition


## Detailed Description
This function is the opcinfo handler for BRIN inclusion operator classes. It allocates and initializes a BrinOpcInfo structure containing all necessary metadata for managing inclusion-based BRIN indexes. The function sets up type cache entries for three key components: the union operation, unmergeable element tracking, and empty element detection. The opaque data structure (InclusionOpaque) is initialized with lazy loading semantics for procedure information arrays, optimizing memory usage and lookup performance.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID of the data type for which the inclusion operator class information is being requested

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [palloc0](../p/palloc0.md)
  - MAXALIGN
  - SizeofBrinOpcInfo
- Data structures:
  - [BrinOpcInfo](../B/BrinOpcInfo.md)
  - [InclusionOpaque](../I/InclusionOpaque.md)
  - [TypeCacheEntry](../T/TypeCacheEntry.md)
- Constants:
  - INCLUSION_UNION
  - INCLUSION_UNMERGEABLE 
  - INCLUSION_CONTAINS_EMPTY
  - BOOLOID
- Called from (representative examples):
  - No direct references found (typically called via operator class framework)

## Notes and Other Information
- The function allocates space for both the BrinOpcInfo structure and the InclusionOpaque data using a single palloc0 call with proper alignment
- All procedure information arrays are initialized lazily with InvalidOid to optimize startup performance
- The oi_nstored field is set to 3, indicating three stored values per BRIN tuple
- Regular null handling is enabled (oi_regular_nulls = true)
- The opaque structure supports cached subtype management for dynamic strategy procedure invalidation