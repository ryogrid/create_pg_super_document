# extract_rollup_sets

## Location
src/backend/optimizer/plan/planner.c: 2980 - 3191

## Overview
Extracts lists of grouping sets that can be implemented using a single rollup-type aggregate pass each, returning the minimal partition of grouping sets organized into chains.

## Definition


## Detailed Description
This function implements an optimal algorithm to partition a list of grouping sets into chains, where each chain can be processed by a single rollup-style aggregate operation. The core problem is finding the minimal partition of a partially-ordered set (ordered by set inclusion) into chains, which is equivalent to finding maximum cardinality matching on a bipartite graph.

The algorithm works by:
1. Stripping out empty sets (which must be in the first result list)
2. Removing duplicate sets to avoid scattered results
3. Building adjacency lists based on subset relationships
4. Using the Hopcroft-Karp bipartite matching algorithm to find optimal partitioning
5. Reconstructing chains from the matching results
6. Re-adding empty sets and duplicates to appropriate chains

The time complexity is O(n^2.5) worst case but typically much better in practice. Planning time for a 12-dimensional cube is under half a second.

## Parameters / Member Variables
- : Input list of grouping sets, must be sorted with smallest sets first

## Dependencies
- Functions called/Symbols referenced:
  - [BipartiteMatch](../B/BipartiteMatch.md)
  - [BipartiteMatchFree](../B/BipartiteMatchFree.md)
  - list_head
  - [lnext](../l/lnext.md)
  - for_each_cell
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_equal](../b/bms_equal.md)
  - [bms_free](../b/bms_free.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [list_concat](../l/list_concat.md)
  - [lcons](../l/lcons.md)
- Called from (representative examples):
  - [preprocess_grouping_sets](../p/preprocess_grouping_sets.md)
  - standard_qp_extra

## Notes and Other Information
- Input must be sorted with smallest sets first, and result maintains this ordering within sublists
- The algorithm is designed to produce the absolute minimum number of lists to avoid excess sorts
- Empty sets are handled specially and always returned in the first list as required by the planner
- Duplicate sets are removed during processing but re-added to appropriate result chains
- Memory management is careful for large sets due to potential for significant memory usage
- Maximum input size is 4096 sets, making polynomial-time algorithms feasible