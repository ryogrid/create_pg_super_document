# range_gist_consistent_leaf_element

## Location
src/backend/utils/adt/rangetypes_gist.c: 1128 - 1147

## Overview
Performs GiST consistent test on an index leaf page with element query, determining whether a stored range contains the specified scalar element.

## Definition
```c
static bool range_gist_consistent_leaf_element(TypeCacheEntry *typcache,
                                               StrategyNumber strategy,
                                               const RangeType *key,
                                               Datum query)
```

## Detailed Description
This function implements the consistent test for GiST index operations on leaf pages when the stored data is a range type and the query is a scalar element (single value). It provides exact containment testing to determine whether the indexed range contains the queried element.

Like its internal node counterpart, this function only supports the RANGESTRAT_CONTAINS_ELEM strategy, which is the only meaningful spatial relationship when querying ranges with scalar elements. The function provides exact matching semantics appropriate for leaf-level operations, directly testing whether the stored range actually contains the element rather than approximating potential containment.

## Parameters / Member Variables
- `typcache`: Type cache entry containing information about the range type being indexed
- `strategy`: Strategy number indicating the type of relationship to test (only RANGESTRAT_CONTAINS_ELEM is supported)
- `key`: The range value stored in this leaf index entry
- `query`: The scalar element value being searched for in the index

## Dependencies
- Functions called/Symbols referenced:
  - [range_contains_elem_internal](range_contains_elem_internal.md)
  - RANGESTRAT_CONTAINS_ELEM
  - elog (for error handling)
- Called from (representative examples):
  - rangeCopy
  - [range_gist_consistent](range_gist_consistent.md)
  - [multirange_gist_consistent](../m/multirange_gist_consistent.md)

## Notes and Other Information
- This is a static function used internally within the range types GiST implementation
- Only supports the RANGESTRAT_CONTAINS_ELEM strategy since other range strategies are not meaningful for element queries
- Provides exact containment testing for leaf-level operations, unlike internal node functions that may be conservative
- The function acts as a strategy dispatcher, delegating to range_contains_elem_internal for the actual containment test
- Used specifically for queries of the form 'which ranges contain element X?' on leaf pages
- Any unsupported strategy results in an ERROR to ensure only valid element containment queries are processed
- This is the final level of the GiST tree where actual stored data is tested rather than approximated