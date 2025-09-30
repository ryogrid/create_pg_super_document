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
  - [SortGroupClause](../S/SortGroupClause.md) (struct type representing sort/group specifications)
  - [list_length](../l/list_length.md) (function to get list length)
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

## Simplified Source

```c
static int common_prefix_cmp(const void *a, const void *b) {
    const WindowClauseSortData *wcsa = a;
    const WindowClauseSortData *wcsb = b;
    ListCell *item_a;
    ListCell *item_b;

    // Compare corresponding sort clauses element by element
    forboth(item_a, wcsa->uniqueOrder, item_b, wcsb->uniqueOrder) {
        SortGroupClause *sca = lfirst_node(SortGroupClause, item_a);
        SortGroupClause *scb = lfirst_node(SortGroupClause, item_b);

        // Compare by tleSortGroupRef (higher refs first)
        if (sca->tleSortGroupRef > scb->tleSortGroupRef)
            return -1;
        else if (sca->tleSortGroupRef < scb->tleSortGroupRef)
            return 1;

        // Compare by sort operator
        else if (sca->sortop > scb->sortop)
            return -1;
        else if (sca->sortop < scb->sortop)
            return 1;

        // Compare by null ordering preference
        else if (sca->nulls_first && !scb->nulls_first)
            return -1;
        else if (!sca->nulls_first && scb->nulls_first)
            return 1;
    }

    // If one is prefix of another, longer list comes first
    if (list_length(wcsa->uniqueOrder) > list_length(wcsb->uniqueOrder))
        return -1;
    else if (list_length(wcsa->uniqueOrder) < list_length(wcsb->uniqueOrder))
        return 1;

    return 0; // Equal
}
```