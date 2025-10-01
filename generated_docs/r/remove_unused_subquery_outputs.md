# remove_unused_subquery_outputs

## Location
[src/backend/optimizer/path/allpaths.c:4055-4166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L4055-L4166)

## Overview
Optimizes subquery performance by removing targetlist items that are not needed by the upper query, potentially allowing for further optimizations like join removal within the subquery.

## Definition

```c
static void
remove_unused_subquery_outputs(Query *subquery, RelOptInfo *rel,
							   Bitmapset *extra_used_attrs)
```
## Detailed Description
This function analyzes a subquery's targetlist and removes output columns that are not referenced by the upper query or required for the subquery's own operation. Rather than physically removing entries (which would affect column numbering), it replaces unused expressions with NULL constants while preserving the original data types.

The optimization is particularly valuable because:
- It can eliminate expensive-to-compute expressions that aren't actually used
- Column deletion may enable further optimizations like join removal within the subquery
- It reduces the amount of data that needs to be processed and transmitted

The function performs several safety checks to ensure semantic correctness, avoiding removal of columns that:
- Have sort/group references (ressortgroupref)
- Are resjunk columns
- Contain set-returning functions (could change row count)
- Contain volatile functions (side effects must be preserved)
- Are part of UNION/INTERSECT/EXCEPT operations
- Are required for DISTINCT operations

## Parameters / Member Variables
- : The Query node representing the subquery to optimize
- : RelOptInfo for the subquery relation, used to determine which columns are actually needed
- : Bitmapset of additional columns (offset by FirstLowInvalidHeapAttributeNumber) that should not be removed; modified by the function

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varattnos](../p/pull_varattnos.md) (extracts variable attribute numbers from expressions)
  - [bms_is_member](../b/bms_is_member.md) (checks bitmap set membership)
  - FirstLowInvalidHeapAttributeNumber (constant for attribute number offset)
  - [expression_returns_set](../e/expression_returns_set.md) (checks if expression returns multiple rows)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for functions with side effects)
  - [makeNullConst](../m/makeNullConst.md) (creates NULL constant with specified type)
  - [exprTypmod](../e/exprTypmod.md) (gets type modifier of expression)
  - [exprCollation](../e/exprCollation.md) (gets collation of expression)
- Called from (representative examples):
  - pushdown_safe_type
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md)

## Notes and Other Information
- The function modifies the subquery's targetList in-place, which is safe because set_subquery_pathlist creates a copy of the subquery
- Column numbering is preserved by replacing unused expressions with NULL constants rather than removing entries
- Whole-row references (attribute 0) prevent any column removal
- The optimization is skipped for set operations (UNION/INTERSECT/EXCEPT) to avoid complexity
- Regular DISTINCT operations require all columns, but DISTINCT ON allows selective removal

## Simplified Source

```c
static void
remove_unused_subquery_outputs(Query *subquery, RelOptInfo *rel,
                              Bitmapset *extra_used_attrs)
{
    Bitmapset *attrs_used = extra_used_attrs;
    ListCell *lc;

    // Skip optimization for set operations
    if (subquery->setOperations)
        return;

    // Skip if regular DISTINCT (not DISTINCT ON)
    if (subquery->distinctClause && !subquery->hasDistinctOn)
        return;

    // Collect all output columns used by upper query
    pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs_used);

    // Add attributes used by restriction clauses
    foreach(lc, rel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs_used);
    }

    // Cannot remove anything if whole-row reference exists
    if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
        return;

    // Process each targetlist entry
    foreach(lc, subquery->targetList)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);
        Node *texpr = (Node *) tle->expr;

        // Keep sort/group columns and resjunk columns
        if (tle->ressortgroupref || tle->resjunk)
            continue;

        // Keep if used by upper query
        if (bms_is_member(tle->resno - FirstLowInvalidHeapAttributeNumber,
                         attrs_used))
            continue;

        // Keep set-returning functions (affect row count)
        if (subquery->hasTargetSRFs && expression_returns_set(texpr))
            continue;

        // Keep volatile functions (preserve side effects)
        if (contain_volatile_functions(texpr))
            continue;

        // Replace unused expression with NULL constant
        tle->expr = (Expr *) makeNullConst(exprType(texpr),
                                         exprTypmod(texpr),
                                         exprCollation(texpr));
    }
}
```