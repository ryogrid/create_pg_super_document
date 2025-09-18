# tlist_same_exprs

## Location
src/backend/optimizer/util/tlist.c: 218 - 247

## Overview
Checks whether two target lists contain the same expressions, ignoring labeling attributes that don't affect computed row values.

## Definition


## Detailed Description
This function compares two target lists to determine if they contain the same expressions. It's primarily used to decide whether it's safe to substitute a new target list into a non-projection-capable plan node. The function performs a structural comparison of the expression trees while ignoring TargetEntry attributes like resname, ressortgroupref, resorigtbl, resorigcol, and resjunk, as these are only labelings that don't affect the actual row values computed by the node.

This design choice allows the optimizer to make more aggressive optimizations by recognizing equivalent computations even when they have different labeling metadata. The planner often doesn't bother to maintain valid labeling in intermediate plan nodes, so ignoring these attributes prevents missed optimization opportunities.

## Parameters / Member Variables
- : First target list to compare
- : Second target list to compare

## Dependencies
- Functions called/Symbols referenced:
  - list_length (implicit via list operations)
  - forboth (macro for iterating over two lists simultaneously)
  - equal (for comparing expression trees)
  - TargetEntry (struct type)
- Called from (representative examples):
  - create_projection_plan
  - change_plan_targetlist
  - standard_qp_extra
  - apply_scanjoin_target_to_paths

## Notes and Other Information
- Returns false immediately if the two lists have different lengths
- On success, the caller must still substitute the desired target list into the plan node to ensure proper labeling
- The function only compares the expr field of each TargetEntry, not the metadata fields
- This is a critical function for query optimization as it enables plan node reuse and target list substitution