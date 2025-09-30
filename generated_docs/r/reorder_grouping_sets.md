# reorder_grouping_sets

## Location
[src/backend/optimizer/plan/planner.c:3192-3239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3192-L3239)

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
  - [list_difference_int](../l/list_difference_int.md)
  - [GroupingSetData](../G/GroupingSetData.md)
  - [SortGroupClause](../S/SortGroupClause.md)
  - [list_nth](../l/list_nth.md)
  - [list_member_int](../l/list_member_int.md)
  - [lappend_int](../l/lappend_int.md)
  - [list_delete_int](../l/list_delete_int.md)
  - [list_concat](../l/list_concat.md)
  - [list_copy](../l/list_copy.md)
  - [lcons](../l/lcons.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [preprocess_grouping_sets](../p/preprocess_grouping_sets.md)
  - standard_qp_extra

## Notes and Other Information
- Input must be ordered with smallest sets first; result is returned with largest sets first
- [Result](../R/Result.md) shares no list substructure with input, making it safe for caller modification
- When a sortclause is provided, the function attempts to follow its column order to minimize unnecessary sorts
- If the algorithm diverges from the sortclause ordering, it abandons the sortclause and proceeds without it
- The prefix relationship established is crucial for rollup aggregate efficiency
- Each result set is wrapped in a GroupingSetData node for further processing

## Simplified Source

```c
static List *
reorder_grouping_sets(List *groupingSets, List *sortclause)
{
    ListCell *lc;
    List *previous = NIL;
    List *result = NIL;

    foreach(lc, groupingSets)
    {
        List *candidate = (List *) lfirst(lc);
        List *new_elems = list_difference_int(candidate, previous);
        GroupingSetData *gs = makeNode(GroupingSetData);

        // Try to follow sortclause order when possible
        while (list_length(sortclause) > list_length(previous) &&
               new_elems != NIL)
        {
            SortGroupClause *sc = list_nth(sortclause, list_length(previous));
            int ref = sc->tleSortGroupRef;

            if (list_member_int(new_elems, ref))
            {
                // Add this element following sortclause order
                previous = lappend_int(previous, ref);
                new_elems = list_delete_int(new_elems, ref);
            }
            else
            {
                // Diverged from sortclause - abandon it
                sortclause = NIL;
                break;
            }
        }

        // Add any remaining new elements
        previous = list_concat(previous, new_elems);

        // Create GroupingSetData with current accumulated set
        gs->set = list_copy(previous);
        result = lcons(gs, result);  // Build in reverse order
    }

    list_free(previous);
    return result;  // Returns largest sets first
}
```