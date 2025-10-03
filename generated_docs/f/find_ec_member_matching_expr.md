# find_ec_member_matching_expr

## Location
[src/backend/optimizer/path/equivclass.c:759-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L759-L832)

## Overview
Locates an EquivalenceMember within an EquivalenceClass that matches a given expression, ignoring binary-compatible RelabelType nodes for flexible matching.

## Definition

```c
EquivalenceMember *
find_ec_member_matching_expr(EquivalenceClass *ec,
							 Expr *expr,
							 Relids relids)
```
## Detailed Description
This function searches through the members of an EquivalenceClass to find one that matches the given expression. The matching logic is designed to be flexible for sort expression identification by ignoring binary-compatible relabeling operations (RelabelType nodes).

Key features of the matching algorithm:

1. **RelabelType Stripping**: Both the target expression and candidate member expressions have RelabelType nodes removed before comparison. This allows matching of expressions that are functionally equivalent but may have different type labels due to binary-compatible casting.

2. **Constant Member Exclusion**: Members marked as constants are ignored since sorting by constant values doesn't make practical sense.

3. **Child Member Filtering**: Child EquivalenceMembers are only considered if their relation set is a subset of the provided relids parameter, ensuring that child members are only matched when they're relevant to the current context.

4. **Exact Expression Matching**: After stripping RelabelType nodes, uses equal() for exact structural comparison of expressions.

This function is commonly used in sort operation planning where the planner needs to determine if a sort expression already has a corresponding EquivalenceMember that could be utilized for optimization purposes.

## Parameters / Member Variables
- `*ec`: EquivalenceClass to search within
- `*expr`: Target expression to find a match for
- `relids`: Relation set limiting which child members can be considered
## Dependencies
- Functions called/Symbols referenced:
  - IsA
  - [bms_is_subset](../b/bms_is_subset.md)
  - [equal](../e/equal.md)
- Called from (representative examples):
  - [relation_can_be_sorted_early](../r/relation_can_be_sorted_early.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)
  - [make_unique_from_pathkeys](../m/make_unique_from_pathkeys.md)

## Notes and Other Information
- Returns NULL if no matching member is found
- [RelabelType](../R/RelabelType.md) stripping enables matching of binary-compatible expressions that may have different exposed types
- Child members must have relids that are a subset of the provided relids parameter to be considered
- Constant members are automatically excluded from consideration as they're not useful for sorting operations
- The function is essential for sort optimization, allowing the planner to reuse existing EquivalenceClass relationships

## Simplified Source

```c
EquivalenceMember *
find_ec_member_matching_expr(EquivalenceClass *ec,
                            Expr *expr,
                            Relids relids)
{
    ListCell   *lc;

    // Strip RelabelType nodes from target expression
    while (expr && IsA(expr, RelabelType))
        expr = ((RelabelType *) expr)->arg;

    // Search through all EC members
    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
        Expr       *emexpr;

        // Skip constant members (not useful for sorting)
        if (em->em_is_const)
            continue;

        // Skip child members unless they belong to requested relations
        if (em->em_is_child && !bms_is_subset(em->em_relids, relids))
            continue;

        // Strip RelabelType nodes from member expression
        emexpr = em->em_expr;
        while (emexpr && IsA(emexpr, RelabelType))
            emexpr = ((RelabelType *) emexpr)->arg;

        // Check for exact match after stripping relabels
        if (equal(emexpr, expr))
            return em;
    }

    return NULL;  // No matching member found
}
```