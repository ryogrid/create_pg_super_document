# build_tlist_index

## Location
src/backend/optimizer/plan/setrefs.c: 2688 - 2738

## Overview
Builds an index data structure for a child targetlist to optimize variable lookups during plan reference resolution.

## Definition


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
  - list_length
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