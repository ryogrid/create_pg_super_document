# find_dependent_phvs_walker

## Location
[src/backend/optimizer/prep/prepjointree.c:3840-3875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3840-L3875)

## Overview
A tree walker function that searches for PlaceHolderVars (PHVs) that depend on a specific set of relations, used to determine if RTE_RESULT removal would leave PHVs without a valid evaluation location.

## Definition
```c
static bool find_dependent_phvs_walker(Node *node, find_dependent_phvs_context *context)
```

## Detailed Description
This function is a specialized tree walker that implements the core logic for detecting PlaceHolderVars that would be affected by RTE_RESULT removal. It traverses expression trees and subqueries looking for PHVs that match specific criteria.

The walker performs several key operations:

**PlaceHolderVar Detection**: When encountering a PHV, it checks two conditions:
1. **Level matching**: The PHV's  must match the current  in the context (ensuring we're looking at PHVs at the correct query nesting level)
2. **Relation matching**: The PHV's  (set of relations where it can be evaluated) must exactly match the target  from the context

**Subquery Handling**: When encountering nested Query nodes, it properly adjusts the  counter to handle PHVs at different nesting levels, then recursively processes the subquery using .

**Node Type Filtering**: The function includes assertions to ensure it doesn't encounter planner auxiliary nodes that should have been handled elsewhere.

The function returns  if it finds any matching PHV, allowing early termination of the search.

## Parameters / Member Variables
- : Current node in the expression tree being examined
- : Context structure containing search criteria:
  - : Target relation set to match against PHV phrels
  - : Current query nesting level for PHV level matching

## Dependencies
- Functions called/Symbols referenced:
  - [bms_equal](../b/bms_equal.md) (to compare relation ID sets)
  - query_tree_walker (for recursing into subqueries)
  - expression_tree_walker (for general expression tree traversal)
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (type checking and access)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md), PlaceHolderInfo, MinMaxAggInfo (assertion checks)
  - find_dependent_phvs_context (parameter type)

- Called from (representative examples):
  - [find_dependent_phvs](find_dependent_phvs.md)
  - [find_dependent_phvs_in_jointree](find_dependent_phvs_in_jointree.md)  
  - [find_dependent_phvs_walker](find_dependent_phvs_walker.md) (recursive self-calls)

## Notes and Other Information
- This is a static function, only accessible within prepjointree.c
- Implements the standard PostgreSQL tree walker pattern with proper recursion handling
- Critical for maintaining query correctness during RTE_RESULT optimization - ensures PHVs retain valid evaluation points
- Handles query nesting levels correctly via sublevels_up tracking
- Uses exact relids matching (bms_equal) rather than subset/superset relationships
- Part of the dependency analysis infrastructure for join tree optimization
- The assertions help catch programming errors where auxiliary planner nodes appear in unexpected contexts
- Returns true on first match for efficiency (early termination)

## Simplified Source

```c
static bool
find_dependent_phvs_walker(Node *node, find_dependent_phvs_context *context)
{
    if (node == NULL)
        return false;

    // Check if this is a matching PlaceHolderVar
    if (IsA(node, PlaceHolderVar))
    {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        // Match level and relation sets exactly
        if (phv->phlevelsup == context->sublevels_up &&
            bms_equal(context->relids, phv->phrels))
            return true;  // Found dependent PHV

        // Continue examining children
    }

    // Handle subqueries with proper level tracking
    if (IsA(node, Query))
    {
        bool result;

        context->sublevels_up++;
        result = query_tree_walker((Query *) node,
                                 find_dependent_phvs_walker,
                                 (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    // Verify we don't encounter unexpected planner nodes
    Assert(!IsA(node, SpecialJoinInfo));
    Assert(!IsA(node, PlaceHolderInfo));
    Assert(!IsA(node, MinMaxAggInfo));

    // Recurse through general expression tree
    return expression_tree_walker(node, find_dependent_phvs_walker,
                                (void *) context);
}
```