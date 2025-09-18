# expand_grouping_sets

## Location
src/backend/parser/parse_agg.c: 1803 - 1905

## Overview
The main orchestration function that expands a list of GroupingSet clauses into a complete, sorted list of grouping combinations through cartesian product calculation, deduplication, and optimization.

## Definition


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