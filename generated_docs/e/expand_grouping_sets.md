# expand_grouping_sets

## Location
[src/backend/parser/parse_agg.c:1803-1905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1803-L1905)

## Overview
The main orchestration function that expands a list of GroupingSet clauses into a complete, sorted list of grouping combinations through cartesian product calculation, deduplication, and optimization.

## Definition

```c
union_int(NIL, (List *) lfirst(lc)));
```
## Detailed Description
This function serves as the primary entry point for expanding complex GROUPING SETS clauses into their final form for query processing. It performs several critical operations:

1. **Individual Expansion**: Calls expand_groupingset_node for each GroupingSet in the input list to get all combinations for that set.

2. **Cartesian Product**: Computes the cartesian product between all expanded grouping sets to create the final set of grouping combinations. For example, if one set expands to [(a), (b)] and another to [(c), (d)], the result includes [(a,c), (a,d), (b,c), (b,d)].

3. **Duplicate Removal**: Uses list_union_int during cartesian product computation to eliminate duplicate column references within individual grouping sets.

4. **Limit Enforcement**: Tracks the total number of combinations and returns NIL if it exceeds the specified limit, preventing exponential explosion.

5. **Sorting and Deduplication**: 
   - When groupDistinct is false: Sorts by length only using cmp_list_len_asc
   - When groupDistinct is true: Sorts individual grouping sets, sorts the entire result by length and contents, then removes duplicate grouping sets

6. **Optimization**: The resulting list is sorted with shorter grouping sets first, which helps the planner optimize GROUP BY processing.

## Parameters / Member Variables
- : List of GroupingSet nodes to expand (ROLLUP, CUBE, GROUPING SETS, etc.)
- : If true, eliminates duplicate grouping sets from the final result
- : Maximum number of grouping combinations allowed (-1 for no limit)

## Dependencies
- Functions called/Symbols referenced:
  - [expand_groupingset_node](expand_groupingset_node.md): Expands individual GroupingSet nodes
  - [list_union_int](../l/list_union_int.md): Combines integer lists while removing duplicates
  - [list_sort](../l/list_sort.md): Sorts lists using comparator functions
  - [cmp_list_len_asc](../c/cmp_list_len_asc.md): Length-based comparator
  - [cmp_list_len_contents_asc](../c/cmp_list_len_contents_asc.md): Length and content-based comparator
  - [list_int_cmp](../l/list_int_cmp.md): Integer comparison for sorting individual grouping sets
  - [equal](equal.md): Checks for duplicate grouping sets
  - foreach_delete_current: Removes duplicates during iteration
- Called from:
  - [parseCheckAggregates](../p/parseCheckAggregates.md): During query parsing for validation
  - [preprocess_grouping_sets](../p/preprocess_grouping_sets.md): In the planner for optimization

## Notes and Other Information
- This function implements the complete SQL standard semantics for GROUPING SETS expansion
- The cartesian product can grow exponentially, hence the limit parameter for protection
- Used both during parsing (for validation) and planning (for optimization)
- The sorting strategy ensures consistent query plans and efficient processing
- Deduplication is only performed when explicitly requested via groupDistinct parameter
- Essential for PostgreSQL's advanced GROUP BY functionality including complex nested grouping operations

## Simplified Source

```c
List *
expand_grouping_sets(List *groupingSets, bool groupDistinct, int limit)
{
    List *expanded_groups = NIL;
    List *result = NIL;
    double numsets = 1;
    ListCell *lc;

    if (groupingSets == NIL)
        return NIL;

    // Phase 1: Expand each grouping set node individually
    foreach(lc, groupingSets)
    {
        GroupingSet *gs = lfirst(lc);
        List *current_result = expand_groupingset_node(gs);

        numsets *= list_length(current_result);

        // Check limit to prevent exponential explosion
        if (limit >= 0 && numsets > limit)
            return NIL;

        expanded_groups = lappend(expanded_groups, current_result);
    }

    // Phase 2: Compute cartesian product
    // Start with first expanded group
    foreach(lc, (List *) linitial(expanded_groups))
    {
        result = lappend(result, list_union_int(NIL, (List *) lfirst(lc)));
    }

    // Combine with remaining groups
    for_each_from(lc, expanded_groups, 1)
    {
        List *current_group = lfirst(lc);
        List *new_result = NIL;
        ListCell *lc2, *lc3;

        foreach(lc2, result)
        {
            List *existing_set = lfirst(lc2);
            foreach(lc3, current_group)
            {
                new_result = lappend(new_result,
                    list_union_int(existing_set, (List *) lfirst(lc3)));
            }
        }
        result = new_result;
    }

    // Phase 3: Sort and optionally deduplicate
    if (!groupDistinct || list_length(result) < 2)
    {
        list_sort(result, cmp_list_len_asc);
    }
    else
    {
        // Sort individual sets, then entire result, then remove duplicates
        ListCell *cell;
        foreach(cell, result)
            list_sort(lfirst(cell), list_int_cmp);

        list_sort(result, cmp_list_len_contents_asc);

        // Remove consecutive duplicates
        List *prev = linitial(result);
        for_each_from(cell, result, 1)
        {
            if (equal(lfirst(cell), prev))
                result = foreach_delete_current(result, cell);
            else
                prev = lfirst(cell);
        }
    }

    return result;
}
```