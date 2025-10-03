# relation_can_be_sorted_early

## Location
[src/backend/optimizer/path/equivclass.c:917-1027](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L917-L1027)

## Overview
Determines whether a relation can be sorted on a given EquivalenceClass before the final output step, during the scan/join part of the plan.

## Definition

```c
bool
relation_can_be_sorted_early(PlannerInfo *root, RelOptInfo *rel,
							 EquivalenceClass *ec, bool require_parallel_safe)
```
## Detailed Description
This function evaluates whether a relation can be sorted early on a specific EquivalenceClass during query execution, before reaching the final output stage. The function employs a two-phase approach: first attempting to find an EC member that directly matches a target expression in the relation's target list, and then trying to find an expression that can be computed from the available target expressions.

The function implements several safety checks to ensure the sort can be performed early, including rejecting volatile expressions, set-returning functions, and optionally non-parallel-safe expressions. Early sorting is beneficial for query optimization as it can enable more efficient join algorithms and reduce overall execution time.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state information
- `*rel`: RelOptInfo structure representing the relation to be potentially sorted
- `*ec`: EquivalenceClass to test for early sortability
- `require_parallel_safe`: If true, non-parallel-safe expressions are rejected
## Dependencies
- Functions called/Symbols referenced:
  - [find_ec_member_matching_expr](../f/find_ec_member_matching_expr.md)
  - [expression_returns_set](../e/expression_returns_set.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - [find_computable_ec_member](../f/find_computable_ec_member.md)
- Called from (representative examples):
  - [get_useful_pathkeys_for_relation](../g/get_useful_pathkeys_for_relation.md)

## Notes and Other Information
- Volatile EquivalenceClasses are immediately rejected as such sorts must be postponed
- Set-returning functions are rejected because they cannot be computed early in the plan
- The function first tries to find direct matches in the relation's target list before attempting computed expressions
- Parallel safety checking is performed last as it's an expensive operation
- Returns true if any suitable EC member is found, false otherwise
- Located in src/backend/optimizer/path/equivclass.c:917-1027

## Simplified Source

```c
bool relation_can_be_sorted_early(PlannerInfo *root, RelOptInfo *rel,
                                  EquivalenceClass *ec, bool require_parallel_safe) {
    PathTarget *target = rel->reltarget;
    EquivalenceMember *em;
    ListCell *lc;

    // Reject volatile ECs immediately
    if (ec->ec_has_volatile)
        return false;

    // Try to find EM directly matching reltarget member
    foreach(lc, target->exprs) {
        Expr *targetexpr = (Expr *) lfirst(lc);

        em = find_ec_member_matching_expr(ec, targetexpr, rel->relids);
        if (!em)
            continue;

        // Reject set-returning functions
        if (expression_returns_set((Node *) em->em_expr))
            continue;

        // Check parallel safety if required
        if (require_parallel_safe && !is_parallel_safe(root, (Node *) em->em_expr))
            continue;

        return true;
    }

    // Try to find computable expression from reltarget
    em = find_computable_ec_member(root, ec, target->exprs, rel->relids, require_parallel_safe);
    if (!em)
        return false;

    // Reject set-returning functions in computed expressions
    if (expression_returns_set((Node *) em->em_expr))
        return false;

    return true;
}
```