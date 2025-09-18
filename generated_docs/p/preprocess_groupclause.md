# preprocess_groupclause

## Location
[src/backend/optimizer/plan/planner.c:2884-2979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2884-L2979)

## Overview
Reorders GROUP BY clause elements to match ORDER BY clause ordering, enabling optimization opportunities for combined sorting and grouping operations.

## Definition
```c
static List *preprocess_groupclause(PlannerInfo *root, List *force)
```

## Detailed Description
This function performs a crucial optimization by rearranging the GROUP BY clause to align with the ORDER BY clause when possible. Since GROUP BY ordering is semantically insignificant, this reordering can provide significant performance benefits:

**Key Optimization Goals:**
1. **Single Sort Operation**: When GROUP BY and ORDER BY have matching prefixes, a single sort can satisfy both requirements
2. **Incremental Sort Support**: Partial matches enable incremental sort optimizations  
3. **Index Optimization**: Reordering can match existing index sort orders

**Algorithm Logic:**

1. **Forced Ordering** (Grouping Sets):
   - When `force` parameter is provided, uses specified ordering for grouping sets
   - Retrieves matching SortGroupClause elements in forced order

2. **Normal Processing**:
   - Returns original order if no ORDER BY clause exists
   - Scans ORDER BY clauses to find matching GROUP BY elements
   - Builds new GROUP BY list maintaining ORDER BY prefix order
   - Stops prefix matching at first non-matching element

3. **Completion**:
   - Adds remaining GROUP BY items to preserve all grouping requirements
   - Validates that all GROUP BY elements have valid sort operators
   - Falls back to original order if any GROUP BY element is non-sortable

## Parameters
- `root`: PlannerInfo structure containing query parse information
- `force`: Optional list of sortgroupref indices to force specific ordering (used for grouping sets)

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupref_clause](../g/get_sortgroupref_clause.md)
  - lfirst_int, list_copy, list_member_ptr
  - [equal](../e/equal.md) (node comparison)
  - OidIsValid
  - SortGroupClause node handling
- Called from:
  - [grouping_planner](../g/grouping_planner.md)
  - standard_qp_extra  
  - [preprocess_grouping_sets](preprocess_grouping_sets.md)
  - [consider_groupingsets_paths](../c/consider_groupingsets_paths.md)

## Notes and Other Information
- **Return Value**: Fresh List containing reordered SortGroupClause elements (same objects as input)
- **Semantic Preservation**: GROUP BY ordering change does not affect query semantics
- **Incremental Sort**: Partial matches still provide optimization benefits through incremental sorting
- **Grouping Sets**: Special handling ensures proper ordering for complex grouping operations  
- **Index Utilization**: Enables better use of existing indexes that match the reordered GROUP BY
- **Parser Integration**: Unlike distinctClause, GROUP BY requires this processing since parser doesnt enforce ORDER BY matching
- Located in src/backend/optimizer/plan/planner.c:2884-2979