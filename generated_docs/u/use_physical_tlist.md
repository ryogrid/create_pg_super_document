# use_physical_tlist

## Location
[src/backend/optimizer/plan/createplan.c:866-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L866-L1002)

## Overview
Determines whether to use a physical target list (matching relation structure) instead of only including variables actually referenced by the query.

## Definition
```c
static bool use_physical_tlist(PlannerInfo *root, Path *path, int flags)
```

## Detailed Description
use_physical_tlist implements the decision logic for determining when it is beneficial to use a "physical" target list that includes all columns from a relation in their natural order, rather than a minimal target list containing only the columns actually referenced by the query. This optimization allows the executor to potentially skip tuple projection operations when the physical tuple layout matches what is needed.

The function performs extensive validation to ensure physical target lists are only used when safe and beneficial. It checks relation types, inheritance situations, system column requirements, placeholder expressions, index-only scan constraints, and sort/group column compatibility. The optimization is particularly valuable for table scans where avoiding projection can improve performance.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner context and global information like placeholder lists
- `path`: The Path node being evaluated for physical target list usage
- `flags`: Control flags affecting the decision (CP_EXACT_TLIST, CP_SMALL_TLIST, CP_LABEL_TLIST)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [bms_nonempty_difference](../b/bms_nonempty_difference.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [CustomPath](../C/CustomPath.md) (type check)
  - [BitmapHeapPath](../B/BitmapHeapPath.md) (type check)
  - [IndexPath](../I/IndexPath.md) (cast)
  - [PlaceHolderInfo](../P/PlaceHolderInfo.md) (type)
  - [IndexOptInfo](../I/IndexOptInfo.md) (type)
  - Various RTE constants (RTE_RELATION, RTE_SUBQUERY, etc.)
  - Various flag constants (CP_EXACT_TLIST, CP_SMALL_TLIST, CP_LABEL_TLIST)
- Called from (representative examples):
  - [create_scan_plan](../c/create_scan_plan.md)
  - [create_projection_plan](../c/create_projection_plan.md)

## Notes and Other Information
- Returns false immediately if CP_EXACT_TLIST or CP_SMALL_TLIST flags are set, as these demand specific target list formats
- Only supports base relations, subqueries, functions, table functions, VALUES, and CTE scans
- Excludes inheritance cases since Append nodes do not project
- Prevents use with CustomPath nodes due to uncertain physical tuple assumptions
- For BitmapHeapScan with empty target lists, maintains the empty list to enable potential heap page fetch skipping
- Rejects cases involving system columns or whole-row variables due to setrefs.c complexity
- Checks placeholder expression requirements to ensure proper evaluation
- For IndexOnlyScan, validates that all index columns are returnable
- When CP_LABEL_TLIST is specified, ensures sort/group columns are simple Vars without conflicts
- Located at src/backend/optimizer/plan/createplan.c:866-1002

## Simplified Source

```c
// Simplified version of use_physical_tlist
static bool use_physical_tlist(PlannerInfo *root, Path *path, int flags) {
    RelOptInfo *rel = path->parent;

    // Quick rejection for exact/small tlist requirements
    if (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST))
        return false;

    // Only support specific relation types
    if (rel->rtekind != RTE_RELATION && rel->rtekind != RTE_SUBQUERY &&
        rel->rtekind != RTE_FUNCTION && rel->rtekind != RTE_TABLEFUNC &&
        rel->rtekind != RTE_VALUES && rel->rtekind != RTE_CTE)
        return false;

    // Must be base relation (no inheritance)
    if (rel->reloptkind != RELOPT_BASEREL)
        return false;

    // Don't use with custom paths or empty bitmap heaps
    if (IsA(path, CustomPath))
        return false;
    if (IsA(path, BitmapHeapPath) && path->pathtarget->exprs == NIL)
        return false;

    // Check for system columns or whole-row vars
    for (int i = rel->min_attr; i <= 0; i++) {
        if (!bms_is_empty(rel->attr_needed[i - rel->min_attr]))
            return false;
    }

    // Check for placeholder expressions that need evaluation
    foreach(ListCell *lc, root->placeholder_list) {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);
        if (bms_nonempty_difference(phinfo->ph_needed, rel->relids) &&
            bms_is_subset(phinfo->ph_eval_at, rel->relids))
            return false;
    }

    // Special handling for index-only scans
    if (path->pathtype == T_IndexOnlyScan) {
        IndexOptInfo *indexinfo = ((IndexPath *) path)->indexinfo;
        for (int i = 0; i < indexinfo->ncolumns; i++) {
            if (!indexinfo->canreturn[i])
                return false;
        }
    }

    // Check sort/group column constraints for labeling
    if ((flags & CP_LABEL_TLIST) && path->pathtarget->sortgrouprefs) {
        Bitmapset *sortgroupatts = NULL;
        int i = 0;
        foreach(ListCell *lc, path->pathtarget->exprs) {
            Expr *expr = (Expr *) lfirst(lc);
            if (path->pathtarget->sortgrouprefs[i]) {
                if (expr && IsA(expr, Var)) {
                    int attno = ((Var *) expr)->varattno - FirstLowInvalidHeapAttributeNumber;
                    if (bms_is_member(attno, sortgroupatts))
                        return false;
                    sortgroupatts = bms_add_member(sortgroupatts, attno);
                } else {
                    return false;
                }
            }
            i++;
        }
    }

    return true;
}
```

Key simplifications made:
- Removed detailed comments explaining each check
- Consolidated related validation checks into logical groups
- Preserved all essential validation logic for safe physical tlist usage
- Maintained the comprehensive checking for various edge cases