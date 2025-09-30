# pull_up_subqueries_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:978-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L978-L1122)

## Overview
The recursive workhorse function that traverses the jointree and performs actual subquery pull-up transformations based on subquery type and context.

## Definition

```c
union_all(rte->subquery))
			return pull_up_simple_union_all(root, jtnode, rte);
```
## Detailed Description
This function implements the core logic for subquery pull-up optimization by recursively traversing the query's jointree and transforming eligible subqueries. It handles multiple types of subquery transformations:

1. **Simple Subquery Pull-up**: Converts simple subqueries (without aggregation, DISTINCT, etc.) into joins by merging their FROM clause into the parent query.

2. **UNION ALL Flattening**: Transforms simple UNION ALL subqueries into append relations for more efficient processing.

3. **VALUES Pull-up**: Inlines simple VALUES expressions when safe to do so.

4. **Function Inlining**: Attempts to inline constant functions.

The function maintains strict rules about when pull-up is safe, considering factors like:
- Outer join context (tracked via )
- Append relation membership (tracked via ) 
- LATERAL dependencies
- Semantic preservation requirements

The recursive traversal handles different jointree node types (RangeTblRef, FromExpr, JoinExpr) and passes context information down to ensure transformations preserve query semantics.

## Parameters / Member Variables
- : PlannerInfo structure containing query context and optimization state
- : Current jointree node being processed
- : Reference to the lowest containing outer join, or NULL if none
- : Reference to containing append relation, or NULL if not within one

## Dependencies
- Functions called/Symbols referenced:
  -  - Prevents stack overflow in deep recursion
  -  - Determines if a subquery is eligible for pull-up
  -  - Checks if append relation member is safe to pull up
  -  - Performs simple subquery pull-up transformation
  -  - Identifies simple UNION ALL structures
  -  - Converts UNION ALL to append relation
  -  - Checks if VALUES RTE is simple enough to inline
  -  - Inlines simple VALUES expressions
  -  - Attempts to inline constant functions
  -  - Retrieves range table entry by index

- Called from (representative examples):
  -  - Entry point for subquery pull-up
  - Self-recursively for jointree traversal
  -  - During UNION ALL processing

## Notes and Other Information
- The function includes stack depth checking and interrupt handling for deep recursion scenarios
- Maintains jointree structural validity during traversal to ensure variable references remain reachable
- Uses different recursion strategies for different join types (INNER vs. outer joins)
- The  parameter constrains LATERAL subquery transformations
- For append relation members, additional safety checks are required via 
- The function modifies the jointree in-place, returning the transformed node
- [Complex](../C/Complex.md) variable substitution is handled by specialized functions like

## Simplified Source

```c
static Node *
pull_up_subqueries_recurse(PlannerInfo *root, Node *jtnode,
                          JoinExpr *lowest_outer_join,
                          AppendRelInfo *containing_appendrel)
{
    // Prevent stack overflow and check for cancellation
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    if (IsA(jtnode, RangeTblRef)) {
        // Handle range table reference (table/subquery/values/function)
        int varno = ((RangeTblRef *) jtnode)->rtindex;
        RangeTblEntry *rte = rt_fetch(varno, root->parse->rtable);

        // Try to pull up simple subquery
        if (rte->rtekind == RTE_SUBQUERY &&
            is_simple_subquery(root, rte->subquery, rte, lowest_outer_join) &&
            (containing_appendrel == NULL || is_safe_append_member(rte->subquery))) {
            return pull_up_simple_subquery(root, jtnode, rte,
                                         lowest_outer_join, containing_appendrel);
        }

        // Try to flatten UNION ALL subquery into append relation
        if (rte->rtekind == RTE_SUBQUERY && is_simple_union_all(rte->subquery)) {
            return pull_up_simple_union_all(root, jtnode, rte);
        }

        // Try to pull up simple VALUES clause
        if (rte->rtekind == RTE_VALUES &&
            lowest_outer_join == NULL &&
            containing_appendrel == NULL &&
            is_simple_values(root, rte)) {
            return pull_up_simple_values(root, jtnode, rte);
        }

        // Try to inline constant function
        if (rte->rtekind == RTE_FUNCTION) {
            return pull_up_constant_function(root, jtnode, rte, containing_appendrel);
        }
    }
    else if (IsA(jtnode, FromExpr)) {
        // Handle FROM clause - recursively process all child nodes
        FromExpr *f = (FromExpr *) jtnode;
        ListCell *l;

        foreach(l, f->fromlist) {
            lfirst(l) = pull_up_subqueries_recurse(root, lfirst(l),
                                                 lowest_outer_join, NULL);
        }
    }
    else if (IsA(jtnode, JoinExpr)) {
        // Handle joins - track outer join context appropriately
        JoinExpr *j = (JoinExpr *) jtnode;

        switch (j->jointype) {
            case JOIN_INNER:
                // Inner joins don't create outer join context
                j->larg = pull_up_subqueries_recurse(root, j->larg, lowest_outer_join, NULL);
                j->rarg = pull_up_subqueries_recurse(root, j->rarg, lowest_outer_join, NULL);
                break;

            case JOIN_LEFT:
            case JOIN_SEMI:
            case JOIN_ANTI:
            case JOIN_FULL:
            case JOIN_RIGHT:
                // Outer joins create new outer join context
                j->larg = pull_up_subqueries_recurse(root, j->larg, j, NULL);
                j->rarg = pull_up_subqueries_recurse(root, j->rarg, j, NULL);
                break;

            default:
                elog(ERROR, "unrecognized join type: %d", (int) j->jointype);
        }
    }
    else {
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));
    }

    return jtnode;
}
```