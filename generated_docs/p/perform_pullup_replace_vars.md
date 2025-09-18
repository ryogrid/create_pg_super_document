# perform_pullup_replace_vars

## Location
src/backend/optimizer/prep/prepjointree.c: 2266 - 2367

## Overview
Performs variable replacement throughout the query tree after subquery pullup, replacing references to the subquery's outputs with copies of adjusted subtlist items.

## Definition


## Detailed Description
This function is the main orchestrator for variable replacement after a subquery has been pulled up into the parent query. It systematically traverses various parts of the query tree (targetList, returningList, havingQual, jointree, etc.) and replaces all references to the pulled-up subquery's outputs with the appropriate replacement expressions.

The function handles two main scenarios:
1. **Appendrel child subquery**: When pulling up a UNION ALL member query, it only processes the translated_vars list of the associated AppendRelInfo, with PHV wrapping disabled.
2. **Regular subquery pullup**: Processes the entire query tree, using PlaceHolderVars (PHVs) appropriately based on the location in the query tree to handle outer join semantics correctly.

The function is careful not to replace any jointree structure itself, delegating that responsibility to  which tracks its location and applies PHVs appropriately.

## Parameters / Member Variables
- : PlannerInfo containing the query being processed
- : Context structure containing substitution mappings and control flags for the replacement operation
- : AppendRelInfo for the containing appendrel if this is a UNION ALL member being pulled up, NULL otherwise

## Dependencies
- Functions called/Symbols referenced:
  - pullup_replace_vars
  - replace_vars_in_jointree
  - pullup_replace_vars_context
  - AppendRelInfo
  - MergeAction
  - RTE_JOIN
- Called from (representative examples):
  - pull_up_simple_subquery
  - pull_up_simple_values
  - pull_up_constant_function

## Notes and Other Information
- Uses PHVs (PlaceHolderVars) in targetList, returningList, and havingQual since these are above any outer join
- For appendrel children, PHV wrapping is disabled since there's no outer join between the child and parent
- Handles special cases like ON CONFLICT clauses, MERGE actions, and join alias variables
- Assumes that ON CONFLICT's arbiterElems, arbiterWhere, and exclRelTlist cannot contain subquery references
- The function asserts that setOperations is NULL, indicating it doesn't handle set operations at this level