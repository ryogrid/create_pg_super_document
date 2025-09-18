# range_gist_consistent_int_element

## Location
[src/backend/utils/adt/rangetypes_gist.c:1039-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1039-L1057)

## Overview
Performs GiST consistent test on an index internal page when the query is a single element, determining whether to descend into subtrees during range index traversal.

## Definition
```c
static bool range_gist_consistent_int_element(TypeCacheEntry *typcache,
                                              StrategyNumber strategy,
                                              const RangeType *key,
                                              Datum query)
```

## Detailed Description
This function implements the consistent test for GiST index operations when searching for ranges that contain a specific element (scalar value) on internal index nodes. Unlike the multirange and range variants, this function only supports a single strategy - RANGESTRAT_CONTAINS_ELEM - which tests whether the range key could contain ranges that include the queried element.

The function is much simpler than its counterparts because element queries only support the containment relationship. It delegates the actual containment test to range_contains_elem_internal and serves as a strategy dispatcher for element-based queries on internal nodes.

## Parameters / Member Variables
- `typcache`: Type cache entry containing information about the range type being indexed
- `strategy`: Strategy number indicating the type of relationship to test (only RANGESTRAT_CONTAINS_ELEM is supported)
- `key`: The range value stored at this internal index node, representing the union of all ranges in the subtree  
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
- Only supports the RANGESTRAT_CONTAINS_ELEM strategy - other range strategies are not applicable when searching for a specific element
- The function acts as a simple strategy dispatcher, with the actual logic contained in range_contains_elem_internal
- Any unsupported strategy results in an ERROR, ensuring that only valid element containment queries are processed
- This function is specifically designed for queries of the form 'which ranges contain element X?'