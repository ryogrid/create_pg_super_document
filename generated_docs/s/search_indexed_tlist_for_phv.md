# search_indexed_tlist_for_phv

## Location
[src/backend/optimizer/plan/setrefs.c:2862-2914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2862-L2914)

## Overview
Searches for a PlaceHolderVar in an indexed target list and returns a Var constructed to reference the matching target list item.

## Definition

```c
static Var *
search_indexed_tlist_for_phv(PlaceHolderVar *phv,
							 indexed_tlist *itlist, int newvarno,
							 NullingRelsMatch nrm_match)
```
## Detailed Description
This function searches through an indexed target list to find a PlaceHolderVar that matches the provided PlaceHolderVar by phid (placeholder ID). When a match is found, it constructs and returns a new Var node that references the target list item containing the matching PlaceHolderVar. The matching is performed based on phid only, not using complete equality checks, both for performance reasons and because phnullingrels might not be exactly equal.

The function includes validation of phnullingrels between the input PlaceHolderVar and the subplan output PlaceHolderVar based on the nrm_match parameter, similar to the corresponding validation in search_indexed_tlist_for_var.

## Parameters / Member Variables
- : The PlaceHolderVar to search for in the indexed target list
- : The indexed target list structure to search within
- : The varno value to assign to the constructed Var if a match is found
- : Controls how phnullingrels are compared (NRM_EQUAL for exact match, NRM_SUBSET/NRM_SUPERSET for partial matches)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - IsA
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [bmsToString](../b/bmsToString.md)
  - elog
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md)
- Data types used:
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - [indexed_tlist](../i/indexed_tlist.md)
  - [TargetEntry](../T/TargetEntry.md)
  - NullingRelsMatch
  - ListCell
- Called from (representative examples):
  - fix_scan_list
  - [fix_join_expr_mutator](../f/fix_join_expr_mutator.md)
  - [fix_upper_expr_mutator](../f/fix_upper_expr_mutator.md)

## Notes and Other Information
- Returns NULL if no matching PlaceHolderVar is found in the indexed target list
- The returned Var has varnosyn and varattnosyn set to 0, indicating it was never a plain Var
- It's recommended to check itlist->has_ph_vars before calling this function, as it's a waste of time to call it otherwise
- Matching is performed on phid only, not complete equality, for both performance and correctness reasons
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2862-2914

## Simplified Source

```c
// Simplified version of search_indexed_tlist_for_phv
static Var *
search_indexed_tlist_for_phv(PlaceHolderVar *placeholder,
                             indexed_tlist *target_list, int new_varno,
                             NullingRelsMatch match_type) {
    ListCell *cell;

    // Search through target list for matching PlaceHolderVar
    foreach(cell, target_list->tlist) {
        TargetEntry *entry = (TargetEntry *) lfirst(cell);

        if (entry->expr && IsA(entry->expr, PlaceHolderVar)) {
            PlaceHolderVar *subplan_phv = (PlaceHolderVar *) entry->expr;

            // Match on placeholder ID only
            if (placeholder->phid != subplan_phv->phid)
                continue;

            // Validate nulling relations based on match type
            bool nulling_rels_valid = false;
            if (match_type == NRM_SUBSET)
                nulling_rels_valid = bms_is_subset(placeholder->phnullingrels,
                                                   subplan_phv->phnullingrels);
            else if (match_type == NRM_SUPERSET)
                nulling_rels_valid = bms_is_subset(subplan_phv->phnullingrels,
                                                   placeholder->phnullingrels);
            else
                nulling_rels_valid = bms_equal(subplan_phv->phnullingrels,
                                               placeholder->phnullingrels);

            if (!nulling_rels_valid)
                elog(ERROR, "wrong phnullingrels for PlaceHolderVar %d", placeholder->phid);

            // Create new Var referencing the target list entry
            Var *new_var = makeVarFromTargetEntry(new_varno, entry);
            new_var->varnosyn = 0;     // Mark as synthetic
            new_var->varattnosyn = 0;
            return new_var;
        }
    }

    return NULL;  // No match found
}
```

Key simplifications made:
- Used more descriptive parameter names for clarity
- Simplified the nulling relations validation logic
- Added comments explaining the matching strategy
- Focused on the core PlaceHolderVar matching and Var creation logic