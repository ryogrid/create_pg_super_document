# add_vars_to_targetlist

## Location
[src/backend/optimizer/plan/initsplan.c:279-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L279-L357)

## Overview
Adds variables from a given list to the appropriate relation targetlists and marks them as needed for specified joins or final output.

## Definition
```c
void add_vars_to_targetlist(PlannerInfo *root, List *vars, Relids where_needed)
```

## Detailed Description
This function processes a list of variables (Var nodes and PlaceHolderVars) and ensures they are properly tracked in the query planning structures. For each variable, it:

1. **For Var nodes**:
   - Finds the owning base relation using find_base_rel()
   - Checks if the variable is already satisfied by existing relations
   - If not already present, adds the variable to the relation's target list
   - Updates the attr_needed bitmap to track where this variable is needed
   - Handles varnullingrels by dropping them at the scan level (to be restored during join tree climbing)

2. **For PlaceHolderVars**:
   - Finds or creates the associated PlaceHolderInfo entry
   - Updates the ph_needed bitmap to indicate where the placeholder is required

The function is essential for building the distributed targetlists that ensure each relation produces the variables needed by higher levels of the join tree.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and state
- `vars`: List of variables (Var and PlaceHolderVar nodes) to be added to targetlists
- `where_needed`: Relids bitmapset indicating which relations need these variables

## Dependencies
- Functions called/Symbols referenced:
  - [find_base_rel](../f/find_base_rel.md)
  - [find_placeholder_info](../f/find_placeholder_info.md)
  - copyObject
  - bms_is_empty
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_add_members](../b/bms_add_members.md)
  - nodeTag
  - lappend
  - elog (for error handling)
- Data structures used:
  - Var
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - PlaceHolderInfo
  - RelOptInfo
- Called from (representative examples):
  - [build_base_rel_tlists](../b/build_base_rel_tlists.md)
  - [extract_lateral_references](../e/extract_lateral_references.md)
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)
  - [process_implied_equality](../p/process_implied_equality.md)

## Notes and Other Information
- The where_needed parameter must not be empty (enforced by Assert)
- [Variables](../V/Variables.md) are copied before being added to targetlists to avoid aliasing issues
- varnullingrels are cleared at the scan level and restored during join processing
- PlaceHolderVars don't have a single owning relation, so their info is kept in root->placeholder_list
- The function includes optimization to skip processing if where_needed is already satisfied
- attr_needed arrays are indexed relative to min_attr for efficiency
- Located in src/backend/optimizer/plan/initsplan.c at lines 279-357