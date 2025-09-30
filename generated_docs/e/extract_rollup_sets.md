# extract_rollup_sets

## Location
[src/backend/optimizer/plan/planner.c:2980-3191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2980-L3191)

## Overview
Extracts lists of grouping sets that can be implemented using a single rollup-type aggregate pass each, returning the minimal partition of grouping sets organized into chains.

## Definition

```c
static List *
extract_rollup_sets(List *groupingSets)
```
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
  - [list_head](../l/list_head.md)
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

## Simplified Source

```c
static List *
extract_rollup_sets(List *groupingSets)
{
    int num_sets_raw = list_length(groupingSets);
    int num_empty = 0;
    int num_sets = 0;
    int num_chains = 0;
    List *result = NIL;
    List **results;
    List **orig_sets;
    Bitmapset **set_masks;
    int *chains;
    short **adjacency;
    BipartiteMatchState *state;
    ListCell *lc1 = list_head(groupingSets);
    ListCell *lc;
    int i, j;

    // Strip out empty sets - they must be in the first result list
    while (lc1 && lfirst(lc1) == NIL)
    {
        ++num_empty;
        lc1 = lnext(groupingSets, lc1);
    }

    if (!lc1)
        return list_make1(groupingSets);

    // Allocate workspace arrays
    orig_sets = palloc0((num_sets_raw + 1) * sizeof(List *));
    set_masks = palloc0((num_sets_raw + 1) * sizeof(Bitmapset *));
    adjacency = palloc0((num_sets_raw + 1) * sizeof(short *));

    i = 1;

    // Build set masks and detect duplicates
    for_each_cell(lc, groupingSets, lc1)
    {
        List *candidate = (List *) lfirst(lc);
        Bitmapset *candidate_set = NULL;
        ListCell *lc2;
        int dup_of = 0;

        // Convert list to bitmapset
        foreach(lc2, candidate)
        {
            candidate_set = bms_add_member(candidate_set, lfirst_int(lc2));
        }

        // Check for duplicates among sets of same length
        // [simplified duplicate detection logic]

        if (dup_of > 0)
        {
            orig_sets[dup_of] = lappend(orig_sets[dup_of], candidate);
            bms_free(candidate_set);
        }
        else
        {
            int k, n_adj = 0;
            short *adjacency_buf = palloc((num_sets_raw + 1) * sizeof(short));

            orig_sets[i] = list_make1(candidate);
            set_masks[i] = candidate_set;

            // Build adjacency list - find all subsets
            for (k = 1; k < i; ++k)
            {
                if (bms_is_subset(set_masks[k], candidate_set))
                    adjacency_buf[++n_adj] = k;
            }

            if (n_adj > 0)
            {
                adjacency_buf[0] = n_adj;
                adjacency[i] = palloc((n_adj + 1) * sizeof(short));
                memcpy(adjacency[i], adjacency_buf, (n_adj + 1) * sizeof(short));
            }

            ++i;
        }
    }

    num_sets = i - 1;

    // Apply bipartite matching algorithm to find optimal chains
    state = BipartiteMatch(num_sets, num_sets, adjacency);

    // Build chains from matching results
    chains = palloc0((num_sets + 1) * sizeof(int));

    for (i = 1; i <= num_sets; ++i)
    {
        int u = state->pair_vu[i];
        int v = state->pair_uv[i];

        if (u > 0 && u < i)
            chains[i] = chains[u];
        else if (v > 0 && v < i)
            chains[i] = chains[v];
        else
            chains[i] = ++num_chains;
    }

    // Build result lists
    results = palloc0((num_chains + 1) * sizeof(List *));

    for (i = 1; i <= num_sets; ++i)
    {
        int c = chains[i];
        results[c] = list_concat(results[c], orig_sets[i]);
    }

    // Add empty sets back to first list
    while (num_empty-- > 0)
        results[1] = lcons(NIL, results[1]);

    // Create final result
    for (i = 1; i <= num_chains; ++i)
        result = lappend(result, results[i]);

    // Cleanup memory
    BipartiteMatchFree(state);
    // [additional cleanup simplified]

    return result;
}
```