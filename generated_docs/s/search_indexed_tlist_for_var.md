# search_indexed_tlist_for_var

## Location
src/backend/optimizer/plan/setrefs.c: 2797 - 2861

## Overview
Searches for a Var node in an indexed target list and returns a modified copy with updated varno/varattno if found.

## Definition


## Detailed Description
This function searches through an indexed target list to find a variable (Var node) that matches the provided varno and varattno. When a match is found, it creates a copy of the original Var with modified varno (set to newvarno) and varattno (set to the resno of the matching target list entry). It also ensures that varnosyn is incremented by rtoffset if it's positive.

The function includes cross-checking of varnullingrels between the input Var and the subplan output Var based on the nrm_match parameter. This validation helps ensure that nulling relation sets are handled correctly during plan tree modifications.

## Parameters / Member Variables
- : The Var node to search for in the indexed target list
- : The indexed target list structure to search within
- : The new varno value to assign to the copied Var if found
- : Offset to add to varnosyn if it's positive
- : Controls how varnullingrels are compared (NRM_EQUAL for exact match, NRM_SUBSET/NRM_SUPERSET for partial matches)

## Dependencies
- Functions called/Symbols referenced:
  - [copyVar](../c/copyVar.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [bmsToString](../b/bmsToString.md)
  - elog
- Data types used:
  - [indexed_tlist](../i/indexed_tlist.md)
  - tlist_vinfo
  - NullingRelsMatch
  - AttrNumber
- Called from (representative examples):
  - fix_scan_list
  - [fix_join_expr_mutator](../f/fix_join_expr_mutator.md)
  - [fix_upper_expr_mutator](../f/fix_upper_expr_mutator.md)

## Notes and Other Information
- Returns NULL if no match is found in the indexed target list
- Skips varnullingrels validation for system columns (varattno <= 0) and whole-row Vars due to complexities with row identity Vars
- The function is part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2797-2861