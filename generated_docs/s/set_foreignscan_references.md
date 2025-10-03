# set_foreignscan_references

## Location
[src/backend/optimizer/plan/setrefs.c:1578-1664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1578-L1664)

## Overview
Adjusts variable references in a ForeignScan plan node during the plan finalization phase to account for range table entry offsets and proper variable referencing.

## Definition

```c
static void
set_foreignscan_references(PlannerInfo *root,
						   ForeignScan *fscan,
						   int rtoffset)
```
## Detailed Description
This function is part of the plan reference adjustment phase in PostgreSQL's query planner. It processes ForeignScan nodes to ensure that all variable references, expressions, and relation IDs are properly adjusted for execution. The function handles two distinct cases:

1. **Custom scan tuple handling**: When the ForeignScan has a custom scan target list (fdw_scan_tlist) or operates without a specific scan relation (scanrelid == 0), it uses fix_upper_expr() to adjust references to point to the foreign scan tuple output.

2. **Standard scan handling**: When using standard relation scanning, it uses fix_scan_list() to adjust references in the conventional manner.

The function ensures that all expressions within the ForeignScan node properly reference the correct variables and relations after plan tree modifications.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context information
- `*fscan`: The ForeignScan plan node whose references need to be adjusted
- `rtoffset`: Range table offset to be applied to relation IDs and variable references
## Dependencies
- Functions called/Symbols referenced:
  - [build_tlist_index](../b/build_tlist_index.md)
  - [fix_upper_expr](../f/fix_upper_expr.md)
  - fix_scan_list
  - [offset_relid_set](../o/offset_relid_set.md)
  - [pfree](../p/pfree.md)
  - NUM_EXEC_TLIST
  - NUM_EXEC_QUAL
  - INDEX_VAR
  - NRM_EQUAL
- Called from (representative examples):
  - [set_plan_refs](set_plan_refs.md)
  - fix_scan_list

## Notes and Other Information
- This is a static function within setrefs.c, indicating it's used internally for plan reference adjustment
- The function handles both custom FDW scan tuple formats and standard relation scanning scenarios
- All FDW-specific expression lists (fdw_exprs, fdw_recheck_quals) are properly adjusted
- [Relation](../R/Relation.md) ID sets (fs_relids, fs_base_relids) are offset to maintain proper relation references
- The resultRelation field is also adjusted if it represents a valid relation

## Simplified Source

```c
static void
set_foreignscan_references(PlannerInfo *root, ForeignScan *fscan, int rtoffset) {
    // Adjust scan relation ID if valid
    if (fscan->scan.scanrelid > 0) {
        fscan->scan.scanrelid += rtoffset;
    }

    if (fscan->fdw_scan_tlist != NIL || fscan->scan.scanrelid == 0) {
        // Custom scan tuple handling: build index and use fix_upper_expr
        indexed_tlist *itlist = build_tlist_index(fscan->fdw_scan_tlist);

        // Fix target list, quals, and FDW expressions to reference foreign scan tuple
        fscan->scan.plan.targetlist = fix_upper_expr(root, fscan->scan.plan.targetlist,
                                                     itlist, INDEX_VAR, rtoffset);
        fscan->scan.plan.qual = fix_upper_expr(root, fscan->scan.plan.qual,
                                              itlist, INDEX_VAR, rtoffset);
        fscan->fdw_exprs = fix_upper_expr(root, fscan->fdw_exprs,
                                         itlist, INDEX_VAR, rtoffset);
        fscan->fdw_recheck_quals = fix_upper_expr(root, fscan->fdw_recheck_quals,
                                                 itlist, INDEX_VAR, rtoffset);

        pfree(itlist);
        fscan->fdw_scan_tlist = fix_scan_list(root, fscan->fdw_scan_tlist, rtoffset);
    } else {
        // Standard scan handling: use fix_scan_list
        fscan->scan.plan.targetlist = fix_scan_list(root, fscan->scan.plan.targetlist, rtoffset);
        fscan->scan.plan.qual = fix_scan_list(root, fscan->scan.plan.qual, rtoffset);
        fscan->fdw_exprs = fix_scan_list(root, fscan->fdw_exprs, rtoffset);
        fscan->fdw_recheck_quals = fix_scan_list(root, fscan->fdw_recheck_quals, rtoffset);
    }

    // Adjust relation ID sets
    fscan->fs_relids = offset_relid_set(fscan->fs_relids, rtoffset);
    fscan->fs_base_relids = offset_relid_set(fscan->fs_base_relids, rtoffset);

    // Adjust result relation if valid
    if (fscan->resultRelation > 0) {
        fscan->resultRelation += rtoffset;
    }
}
```