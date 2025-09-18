# reorder_grouping_sets

## Location
src/backend/optimizer/plan/planner.c: 3192 - 3239

## Overview
Reorders elements of grouping sets to establish correct prefix relationships and inserts GroupingSetData annotations, optimizing for minimal sorting overhead.

## Definition
```c
static List *reorder_grouping_sets(List *groupingSets, List *sortclause)
```

## Detailed Description
This function transforms a list of grouping sets by reordering their elements to establish proper prefix relationships, which is essential for efficient rollup-style aggregate processing. The function also wraps each grouping set in a GroupingSetData structure.

The algorithm works by:
1. Processing grouping sets in order (smallest to largest)
2. For each set, determining new elements not in the previous accumulated set
3. Following the sortclause column order when possible to minimize additional sorts
4. Building each new set as an extension of the previous set (prefix relationship)
5. Wrapping results in GroupingSetData nodes
6. Returning results in reverse order (largest sets first)

The prefix relationship ensures that each grouping set contains all elements of the previous set plus additional elements, enabling efficient single-pass rollup processing.

## Parameters / Member Variables
- `groupingSets`: Input list of grouping sets, ordered with smallest sets first
- `sortclause`: Optional sort clause to follow for column ordering to minimize additional sorts

## Dependencies
- Functions called/Symbols referenced:
  - list_difference_int
  - GroupingSetData
  - SortGroupClause
  - list_nth
  - list_member_int
  - lappend_int
  - list_delete_int
  - list_concat
  - list_copy
  - lcons
  - list_free
- Called from (representative examples):
  - preprocess_grouping_sets
  - standard_qp_extra

## Notes and Other Information
- Input must be ordered with smallest sets first; result is returned with largest sets first
- Result shares no list substructure with input, making it safe for caller modification
- When a sortclause is provided, the function attempts to follow its column order to minimize unnecessary sorts
- If the algorithm diverges from the sortclause ordering, it abandons the sortclause and proceeds without it
- The prefix relationship established is crucial for rollup aggregate efficiency
- Each result set is wrapped in a GroupingSetData node for further processing