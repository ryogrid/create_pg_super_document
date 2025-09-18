# Ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:169-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L169-L196)

## Overview
Ranges is a core data structure for BRIN minmax-multi indexes that represents a collection of value ranges and single-point values in an efficient in-memory format optimized for processing operations.

## Definition


## Detailed Description
Ranges is the in-memory representation used by BRIN minmax-multi indexes for efficient processing of range operations. It stores boundary values in a hybrid format: regular ranges (storing both lower and upper bounds) followed by single-point values. The structure uses a flexible array layout where the first 2*nranges elements represent range boundaries (pairs of lower/upper bounds), followed by nvalues single-point values. This design allows efficient addition of new values and storage of outliers without widening existing ranges. The structure includes caching of frequently-used metadata (type info, comparison functions) and supports incremental sorting with the nsorted counter tracking how many single-point values are currently sorted.

## Parameters / Member Variables
- `typid`: Object identifier of the data type being indexed, cached for performance
- `colloid`: Object identifier of the collation to use for comparisons, cached for performance  
- `attno`: Attribute number of the indexed column
- `cmp`: Pointer to cached comparison function manager info for efficient value comparisons
- `nranges`: Number of regular ranges stored in the values array (each range uses 2 array slots)
- `nsorted`: Number of single-point values that are currently sorted (0 <= nsorted <= nvalues)
- `nvalues`: Total number of single-point values stored after the range data
- `maxvalues`: Total capacity of the values array (2*nranges + nvalues <= maxvalues)
- `target_maxvalues`: Target number of values for compaction operations
- `values[FLEXIBLE_ARRAY_MEMBER]`: Flexible array storing range boundaries followed by single-point values

## Dependencies
- Constants referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Used extensively by functions:
  - [AssertCheckRanges](../A/AssertCheckRanges.md)
  - [minmax_multi_init](../m/minmax_multi_init.md)
  - [range_deduplicate_values](../r/range_deduplicate_values.md)
  - [brin_range_serialize](../b/brin_range_serialize.md)/deserialize
  - [has_matching_range](../h/has_matching_range.md)
  - [range_contains_value](../r/range_contains_value.md)
  - [range_add_value](../r/range_add_value.md)
  - [compactify_ranges](../c/compactify_ranges.md)
  - brin_minmax_multi_* functions

## Notes and Other Information
- The values array layout is: [range1_lower, range1_upper, range2_lower, range2_upper, ...ranges..., point1, point2, ...points...]
- Designed to minimize palloc overhead by pre-allocating space for maxvalues elements
- Supports incremental operations with deferred sorting/deduplication for performance
- The nsorted field enables efficient partial sorting operations
- Part of a two-tier system with SerializedRanges for persistent storage
- Optimized for the common case where values_per_range reloption is reasonably small (typically <= 256)