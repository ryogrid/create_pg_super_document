# consider_index_join_outer_rels

## Location
[src/backend/optimizer/path/indxpath.c:497-599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L497-L599)

## Overview
Generates parameterized index paths by systematically examining combinations of outer relation sets from join clauses, implementing the core logic for parameterized path enumeration with heuristic limits to prevent exponential explosion.

## Definition

```c
union of this clause's relids set with each
		 * previously-tried set.  This ensures we try this clause along with
		 * every interesting subset of previous clauses.  However, to avoid
		 * exponential growth of planning time when there are many clauses,
		 * limit the number of relid sets accepted to 10 * considered_clauses.
		 *
		 * Note: get_join_index_paths appends entries to *considered_relids,
		 * but we do not need to visit such newly-added entries within this
		 * loop, so we don't use foreach() here.  No real harm would be done
		 * if we did visit them, since the subset check would reject them;
```
## Detailed Description
This function serves as the workhorse for consider_index_join_clauses, implementing the detailed logic for generating parameterized index paths. For each join clause in the input list, it:

1. **Extracts relation sets**: Gets the clause_relids from each IndexClause to understand which relations are involved
2. **Avoids redundancy**: Skips relation sets already processed (tracked in considered_relids)
3. **Generates combinations**: Creates union sets by combining the current clause's relids with each previously-tried set, ensuring exploration of useful clause combinations
4. **Applies heuristic limits**: Caps the number of relation sets at 10 * considered_clauses to prevent exponential growth in planning time
5. **Handles EquivalenceClasses**: Uses eclass_already_used() to avoid redundant combinations when clauses derive from the same EquivalenceClass
6. **Delegates path creation**: Calls get_join_index_paths() for each viable relation set to actually generate the paths

The function implements both combination logic (trying clauses together) and individual processing (trying each clause alone).

## Parameters / Member Variables
- : PlannerInfo containing query planning context
- : RelOptInfo for the index's heap relation
- : IndexOptInfo for the index to generate paths for
- : IndexClauseSet containing indexable restriction clauses
- : IndexClauseSet containing indexable simple join clauses  
- : IndexClauseSet containing indexable EquivalenceClass clauses
- : Output list for bitmap index paths
- : List of IndexClauses for join clauses to process
- : Total count of clauses considered (for heuristic limit)
- : Input/output list tracking all relation sets already processed

## Dependencies
- Functions called/Symbols referenced:
  - [bms_subset_compare](../b/bms_subset_compare.md)
  - [eclass_already_used](../e/eclass_already_used.md)
  - [get_join_index_paths](../g/get_join_index_paths.md)
  - [bms_union](../b/bms_union.md)
  - [list_member](../l/list_member.md)
  - [list_nth](../l/list_nth.md)
- Called from (representative examples):
  - [consider_index_join_clauses](consider_index_join_clauses.md)

## Notes and Other Information
- Uses BMS_DIFFERENT check to avoid subset relationships that wouldn't generate new information
- Implements a 10 * considered_clauses heuristic limit to prevent exponential planning time growth
- Carefully avoids revisiting newly-added entries in considered_relids during the same loop iteration
- Handles both EquivalenceClass-derived clauses and regular join clauses uniformly
- The subset check (bms_subset_compare) is a quick redundancy filter; get_join_index_paths performs more thorough duplicate detection
- Always tries each clause's relation set individually, even when combination limits are exceeded

## Simplified Source

```c
static void
consider_index_join_outer_rels(PlannerInfo *root, RelOptInfo *rel,
                               IndexOptInfo *index,
                               IndexClauseSet *rclauseset,
                               IndexClauseSet *jclauseset,
                               IndexClauseSet *eclauseset,
                               List **bitindexpaths,
                               List *indexjoinclauses,
                               int considered_clauses,
                               List **considered_relids)
{
    ListCell *lc;

    // Process each join clause in the list
    foreach(lc, indexjoinclauses)
    {
        IndexClause *iclause = (IndexClause *) lfirst(lc);
        Relids clause_relids = iclause->rinfo->clause_relids;
        EquivalenceClass *parent_ec = iclause->rinfo->parent_ec;
        int num_considered_relids;

        // Skip if we already tried this relids set
        if (list_member(*considered_relids, clause_relids))
            continue;

        // Generate combinations with previously-tried sets
        num_considered_relids = list_length(*considered_relids);
        for (int pos = 0; pos < num_considered_relids; pos++)
        {
            Relids oldrelids = (Relids) list_nth(*considered_relids, pos);

            // Skip if one is subset of the other (no new info)
            if (bms_subset_compare(clause_relids, oldrelids) != BMS_DIFFERENT)
                continue;

            // Skip if equivalence class already used with oldrelids
            if (parent_ec && eclass_already_used(parent_ec, oldrelids, indexjoinclauses))
                continue;

            // Apply heuristic limit to prevent exponential growth
            if (list_length(*considered_relids) >= 10 * considered_clauses)
                break;

            // Try the union of current and old relids
            get_join_index_paths(root, rel, index,
                                rclauseset, jclauseset, eclauseset,
                                bitindexpaths,
                                bms_union(clause_relids, oldrelids),
                                considered_relids);
        }

        // Also try this clause's relids by itself
        get_join_index_paths(root, rel, index,
                            rclauseset, jclauseset, eclauseset,
                            bitindexpaths,
                            clause_relids,
                            considered_relids);
    }
}
```