# varstrfastcmp_locale

## Location
src/backend/utils/adt/varlena.c: 2139 - 2238

## Overview
The core locale-aware string comparison function used by PostgreSQL's sort support framework, implementing optimized string comparison with caching and locale-specific collation rules.

## Definition


## Detailed Description
 is the fundamental locale-aware string comparison function in PostgreSQL's sort support infrastructure. This function implements several key optimizations for string comparison during sorting operations:

1. **Fast equality pre-check**: Uses memcmp() for quick detection of identical strings
2. **BPCHAR handling**: Special processing for blank-padded character types, removing trailing spaces
3. **Intelligent caching**: Maintains buffers with previously compared strings to avoid expensive strcoll() calls when comparing the same strings repeatedly
4. **Dynamic buffer management**: Grows comparison buffers as needed to accommodate longer strings
5. **Locale-aware comparison**: Uses pg_strcoll() for proper collation according to locale rules
6. **Deterministic tie-breaking**: Falls back to strcmp() for deterministic ordering when locale comparison returns equality

The function is designed to be highly efficient for sorting operations where the same strings are often compared multiple times, such as during quicksort partitioning.

## Parameters / Member Variables
- : Pointer to the first string to compare
- : Length of the first string in bytes
- : Pointer to the second string to compare  
- : Length of the second string in bytes
- : SortSupport structure containing VarStringSortSupport context with buffers, cache state, and locale information

## Dependencies
- Functions called/Symbols referenced:
  -  - Support structure type for string sorting operations
  -  - Fast binary comparison for equality checks and cache validation
  -  - Calculates true length of BPCHAR excluding trailing spaces
  - ,  - Macros for buffer size calculations
  -  - PostgreSQL constant for maximum allocation size
  -  - PostgreSQL memory reallocation function
  -  - Copies string data to comparison buffers
  -  - PostgreSQL locale-aware string comparison function
  -  - Checks if locale provides deterministic comparison
  -  - Standard C string comparison for tie-breaking
- Called from (representative examples):
  -  - Wrapper for varlena types
  -  - Wrapper for NAME types

## Notes and Other Information
- Central to PostgreSQL's string sorting performance optimization
- Implements sophisticated caching strategy that can significantly speed up sorting with repeated string comparisons
- Handles both regular string types and blank-padded character (BPCHAR) types with different trailing space semantics
- Buffer management grows exponentially up to MaxAllocSize to balance memory usage and reallocation overhead
- Cache effectiveness depends on sorting patterns - most beneficial with low to moderate cardinality data sets
- The function ensures deterministic results even with non-deterministic locales by using strcmp() as a tie-breaker