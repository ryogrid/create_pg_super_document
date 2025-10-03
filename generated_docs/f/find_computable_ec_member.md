# find_computable_ec_member

## Location
[src/backend/optimizer/path/equivclass.c:833-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L833-L916)

## Overview
Locates an EquivalenceClass member that can be computed from a given list of expressions, returning NULL if no match is found.

## Definition

```c
EquivalenceMember *
find_computable_ec_member(PlannerInfo *root,
						  EquivalenceClass *ec,
						  List *exprs,
						  Relids relids,
						  bool require_parallel_safe)
```
## Detailed Description
This function searches through an EquivalenceClass to find a member expression that can be computed using the variables and functions present in the provided expressions list. The function considers an EC member computable if all the Vars, PlaceHolderVars, Aggrefs, and WindowFuncs it needs are present in the input expressions.

The function supports some flexibility in expression matching - for example, if an EC member is "Var_A + 1" while the input contains "Var_A + 2", it's still considered computable because both expressions can use the same underlying variable in the final plan tree.

Unlike find_ec_member_matching_expr, this function does not provide special handling for binary-compatible relabeling, as setrefs.c requires exact matches of Vars to the source targetlist when computing expressions this way.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state (can be NULL when require_parallel_safe is false)
- `*ec`: The EquivalenceClass to search through for computable members
- `*exprs`: List of expressions (can be bare expression trees or TargetEntry nodes) that define what variables/functions are available
- `relids`: Set of relation IDs - child EC members are only considered if they belong to these relations
- `require_parallel_safe`: If true, non-parallel-safe expressions are ignored
## Dependencies
- Functions called/Symbols referenced:
  - [pull_var_clause](../p/pull_var_clause.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [list_member](../l/list_member.md)
  - [list_free](../l/list_free.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
- Called from (representative examples):
  - [relation_can_be_sorted_early](../r/relation_can_be_sorted_early.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)

## Notes and Other Information
- Child EC members are ignored unless they belong to the specified relids
- Constant EC members are skipped as they shouldn't be used for sorting
- The function extracts variables using PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags
- Parallel safety checking is performed last as it's an expensive operation
- Located in src/backend/optimizer/path/equivclass.c:833-916

## Simplified Source

```c
EquivalenceMember *
find_computable_ec_member(PlannerInfo *root,
                         EquivalenceClass *ec,
                         List *exprs,
                         Relids relids,
                         bool require_parallel_safe)
{
    List       *exprvars;
    ListCell   *lc;

    // Extract all variables and quasi-variables from input expressions
    exprvars = pull_var_clause((Node *) exprs,
                              PVC_INCLUDE_AGGREGATES |
                              PVC_INCLUDE_WINDOWFUNCS |
                              PVC_INCLUDE_PLACEHOLDERS);

    // Check each EC member to see if it's computable
    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
        List       *emvars;
        ListCell   *lc2;

        // Skip constant members (shouldn't be used for sorting)
        if (em->em_is_const)
            continue;

        // Skip child members unless they belong to requested relations
        if (em->em_is_child && !bms_is_subset(em->em_relids, relids))
            continue;

        // Check if all variables in this EC member are available in exprs
        emvars = pull_var_clause((Node *) em->em_expr,
                                PVC_INCLUDE_AGGREGATES |
                                PVC_INCLUDE_WINDOWFUNCS |
                                PVC_INCLUDE_PLACEHOLDERS);

        foreach(lc2, emvars)
        {
            if (!list_member(exprvars, lfirst(lc2)))
                break;  // Found a variable that's not available
        }
        list_free(emvars);

        if (lc2)
            continue;  // Some variables were missing

        // Check parallel safety if required (expensive check done last)
        if (require_parallel_safe &&
            !is_parallel_safe(root, (Node *) em->em_expr))
            continue;

        return em;  // Found a computable expression
    }

    return NULL;  // No computable member found
}
```