# build_tlist_index

## Location
[src/backend/optimizer/plan/setrefs.c:2688-2738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2688-L2738)

## Overview
Builds an index data structure for a child targetlist to optimize variable lookups during plan reference resolution.

## Definition

```c
structure with enough slots for all tlist entries */
	itlist = (indexed_tlist *)
		palloc(offsetof(indexed_tlist, vars) +
			   list_length(tlist) * sizeof(tlist_vinfo));
```
## Detailed Description
This function creates an optimized index structure for targetlist matching operations. Since subplan targetlists are typically "flat" containing mostly Vars, the function pre-extracts variable information into an array for faster lookups. While targetlist matching is still an O(N^2) operation when matching parent to child tlists, this indexing provides a much smaller constant factor compared to plain tlist_member() searches.

The resulting indexed_tlist structure contains:
- The original targetlist reference
- An array of variable information (varno, varattno, resno, varnullingrels) for quick access
- Flags indicating presence of PlaceHolderVars and non-Var expressions
- Count of variables found

This optimization is particularly effective for the common case where targetlists consist primarily of simple variable references, which is typical in most query plans.

## Parameters / Member Variables
- : The targetlist (List of TargetEntry nodes) to be indexed

## Dependencies
- Functions called/Symbols referenced:
  - palboc (with offsetof calculation)
  - [list_length](../l/list_length.md)
  - lfirst (list iteration macro)
  - IsA (type checking macro)
  - offsetof (for structure size calculation)
- Called from (representative examples):
  - fix_scan_list (src/backend/optimizer/plan/setrefs.c:168)
  - [set_plan_refs](../s/set_plan_refs.md) (src/backend/optimizer/plan/setrefs.c:1114, 1167)
  - [set_indexonlyscan_references](../s/set_indexonlyscan_references.md) (src/backend/optimizer/plan/setrefs.c:1345)
  - [set_foreignscan_references](../s/set_foreignscan_references.md) (src/backend/optimizer/plan/setrefs.c:1592)
  - [set_customscan_references](../s/set_customscan_references.md) (src/backend/optimizer/plan/setrefs.c:1678)
  - [set_hash_references](../s/set_hash_references.md) (src/backend/optimizer/plan/setrefs.c:1912)
  - [set_join_references](../s/set_join_references.md) (src/backend/optimizer/plan/setrefs.c:2289, 2290)
  - [set_upper_references](../s/set_upper_references.md) (src/backend/optimizer/plan/setrefs.c:2438)
  - [set_windowagg_runcondition_references](../s/set_windowagg_runcondition_references.md) (src/backend/optimizer/plan/setrefs.c:3419)

## Notes and Other Information
- Optimizes the common case of flat targetlists containing mostly Vars
- Uses a variable-length structure allocated with a single palloc() call
- The entire indexed_tlist structure can be freed with a single pfree()
- Tracks different types of expressions: regular Vars, PlaceHolderVars, and other non-Var expressions
- The varnullingrels field is preserved for proper null handling in outer joins
- Designed to work with companion functions like search_indexed_tlist_for_var()
- Provides significant performance improvement for plan reference resolution in complex queries

## Simplified Source

```c
static indexed_tlist *build_tlist_index(List *tlist) {
    // Allocate indexed structure with space for all entries
    indexed_tlist *itlist = (indexed_tlist *)
        palloc(offsetof(indexed_tlist, vars) +
               list_length(tlist) * sizeof(tlist_vinfo));

    // Initialize the structure
    itlist->tlist = tlist;
    itlist->has_ph_vars = false;
    itlist->has_non_vars = false;

    // Extract variable information from targetlist
    tlist_vinfo *vinfo = itlist->vars;
    ListCell *l;

    foreach(l, tlist) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        if (tle->expr && IsA(tle->expr, Var)) {
            // Store variable details for fast lookup
            Var *var = (Var *) tle->expr;
            vinfo->varno = var->varno;
            vinfo->varattno = var->varattno;
            vinfo->resno = tle->resno;
            vinfo->varnullingrels = var->varnullingrels;
            vinfo++;
        }
        else if (tle->expr && IsA(tle->expr, PlaceHolderVar)) {
            itlist->has_ph_vars = true;
        }
        else {
            itlist->has_non_vars = true;
        }
    }

    itlist->num_vars = (vinfo - itlist->vars);
    return itlist;
}
```