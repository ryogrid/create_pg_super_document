# common_prefix_cmp

## Location
[src/backend/optimizer/plan/planner.c:6012-6080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6012-L6080)

## Overview
QSort comparison function for WindowClauseSortData that sorts window clauses by their sorting requirements, prioritizing higher tleSortGroupRefs and placing windows with more sort clauses first when one is a prefix of another.

## Definition
```c
static int common_prefix_cmp(const void *a, const void *b)
```

## Detailed Description
This comparison function implements a sophisticated sorting strategy for window clauses that serves two primary optimization goals. First, it ensures that windows with similar sorting requirements are grouped together to minimize the number of sort operations during execution. Second, it strategically orders windows to maximize the likelihood that later operations (like DISTINCT or ORDER BY) can benefit from presorted input.

The comparison logic proceeds in multiple phases:
1. Compares corresponding SortGroupClauses by tleSortGroupRef in descending order
2. If tleSortGroupRefs match, compares sort operators 
3. If sort operators match, compares null ordering preferences
4. When one window's clauses are a prefix of another's, places the window with more clauses first

The strategic ordering by highest tleSortGroupRef first is designed to align with PostgreSQL's assignment strategy where DISTINCT and ORDER BY clauses receive the lowest tleSortGroupRefs, thereby increasing the chances that window processing will produce presorted input for subsequent query operations.

## Parameters / Member Variables
- `a`: Pointer to first WindowClauseSortData structure for comparison
- `b`: Pointer to second WindowClauseSortData structure for comparison

## Dependencies
- Functions called/Symbols referenced:
  - WindowClauseSortData (struct type containing window clause and uniqueOrder)
  - forboth (macro for parallel iteration over two lists)
  - SortGroupClause (struct type representing sort/group specifications)
  - list_length (function to get list length)
  - lfirst_node (macro to access list node content)
- Called from (representative examples):
  - [select_active_windows](../s/select_active_windows.md) (src/backend/optimizer/plan/planner.c:5984)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:256)

## Notes and Other Information
- Implements a prefix-aware comparison where windows with more comprehensive sorting requirements take precedence
- The tleSortGroupRef comparison strategy is specifically designed to optimize integration with query-level DISTINCT and ORDER BY operations
- Equality operator (eqop) comparison is omitted as it's fully determined by the sort operator (sortop)
- Returns standard qsort comparison values: -1 (a < b), 0 (a == b), 1 (a > b)
- Static function scope indicates it's a specialized utility for window clause sorting within the planner module
- The function assumes that both input structures have valid uniqueOrder lists for comparison