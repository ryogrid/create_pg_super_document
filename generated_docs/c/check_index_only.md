# check_index_only

## Location
[src/backend/optimizer/path/indxpath.c:1730-1828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1730-L1828)

## Overview
Determines whether an index-only scan is possible for a given index by checking if all required attributes can be retrieved from the index itself.

## Definition
```c
static bool check_index_only(RelOptInfo *rel, IndexOptInfo *index)
```

## Detailed Description
This function analyzes whether an index-only scan can be performed for a specific index on a relation. An index-only scan is an optimization where the query executor can satisfy a query using only the index data without accessing the main table heap. The function performs several checks:

1. Verifies that index-only scans are enabled globally via enable_indexonlyscan
2. Identifies all attributes needed for joins, final output, and restriction clauses
3. Constructs a bitmap of columns that the index can return
4. Determines if all necessary attributes are available from the index

The function considers both the relation's target list and restriction clauses, excluding those implied by index predicates since they don't need explicit checking.

## Parameters / Member Variables
- `rel`: RelOptInfo pointer representing the relation being queried
- `index`: IndexOptInfo pointer containing metadata about the index being evaluated

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varattnos](../p/pull_varattnos.md) (to extract attribute numbers from expressions)
  - [bms_add_member](../b/bms_add_member.md) (to add attributes to bitmap sets)
  - [bms_is_subset](../b/bms_is_subset.md) (to check if required attributes are subset of available)
  - [bms_free](../b/bms_free.md) (to free bitmap memory)
  - FirstLowInvalidHeapAttributeNumber (constant for attribute number conversion)
- Called from (representative examples):
  - ec_member_matches_arg
  - [build_index_paths](../b/build_index_paths.md)

## Notes and Other Information
- Index expressions are currently ignored but may be supported in future versions
- The function uses bitmap sets to efficiently track and compare attribute requirements
- Index-only scans are a significant performance optimization as they avoid heap access
- The check considers inheritance child relations by examining the relation's target list directly rather than attr_needed data