# substitute_phv_relids_walker

## Location
[src/backend/optimizer/prep/prepjointree.c:3965-4008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3965-L4008)

## Overview
A tree walker function that modifies PlaceHolderVar relation ID sets in-place by substituting one relation ID with a set of subrelation IDs.

## Definition

```c
union(phv->phrels,
									context->subrelids);
```
## Detailed Description
This walker function traverses expression trees and modifies PlaceHolderVar nodes in-place to update their relation ID bitmapsets. When it encounters a PlaceHolderVar that references a specific relation ID (context->varno), it replaces that relation ID with a set of subrelation IDs (context->subrelids).

The function performs the substitution by first adding all the subrelation IDs to the PHV's phrels bitmapset using bms_union, then removing the original relation ID using bms_del_member. This effectively replaces a single relation reference with multiple subrelation references.

The function includes safety assertions to ensure it doesn't encounter planner auxiliary nodes that shouldn't be present at this stage, and validates that PHVs are never left empty after the substitution.

NOTE: This function modifies nodes in-place, which is safe because the tree was previously copied by pullup_replace_vars. However, it avoids modifying the original bitmapset values since expression_tree_mutator doesn't copy those.

## Parameters / Member Variables
- : The current node being examined in the tree traversal
- : Context structure containing substitution parameters:
  - : The relation ID to be replaced  
  - : Current query nesting level for PHV matching
  - : The set of relation IDs to substitute for varno

## Dependencies
- Functions called/Symbols referenced:
  - substitute_phv_relids_context (context structure)
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (node type for placeholder variables)
  - [bms_is_member](../b/bms_is_member.md) (checks if relation ID is in bitmapset)
  - [bms_union](../b/bms_union.md) (combines two bitmapsets)
  - [bms_del_member](../b/bms_del_member.md) (removes relation ID from bitmapset)
  - bms_is_empty (checks if bitmapset is empty)
  - query_tree_walker (recursively processes subqueries)
  - expression_tree_walker (recursively processes expressions)
- Called from (representative examples):
  - [substitute_phv_relids_walker](substitute_phv_relids_walker.md) (recursive self-calls for subqueries and expressions)
  - [substitute_phv_relids](substitute_phv_relids.md) (main entry point function)

## Notes and Other Information
- This function is static and only used within prepjointree.c
- Modifies nodes in-place rather than creating copies (performance optimization)
- Includes assertions to prevent handling of planner auxiliary nodes (SpecialJoinInfo, AppendRelInfo, PlaceHolderInfo, MinMaxAggInfo)
- Part of the append relation processing where individual table references are replaced with references to their constituent partitions or inheritance children
- The in-place modification is safe due to prior tree copying by pullup_replace_vars
- Maintains PHV integrity by ensuring phrels is never left empty after substitution

## Simplified Source

```c
static bool substitute_phv_relids_walker(Node *node, substitute_phv_relids_context *context) {
    if (node == NULL)
        return false;

    // Handle PlaceHolderVar nodes - replace relation IDs
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        // Check if this PHV references the target relation at correct level
        if (phv->phlevelsup == context->sublevels_up &&
            bms_is_member(context->varno, phv->phrels)) {

            // Replace varno with subrelids
            phv->phrels = bms_union(phv->phrels, context->subrelids);
            phv->phrels = bms_del_member(phv->phrels, context->varno);

            Assert(!bms_is_empty(phv->phrels));
        }
    }

    // Handle subqueries with adjusted nesting level
    if (IsA(node, Query)) {
        context->sublevels_up++;
        bool result = query_tree_walker((Query *) node, substitute_phv_relids_walker,
                                      (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    // Continue walking the expression tree
    return expression_tree_walker(node, substitute_phv_relids_walker, (void *) context);
}
```