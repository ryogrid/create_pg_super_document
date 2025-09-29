# set_customscan_references

## Location
[src/backend/optimizer/plan/setrefs.c:1665-1740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1665-L1740)

## Overview
Adjusts variable references in a CustomScan plan node during the plan finalization phase to account for range table entry offsets and proper variable referencing, including recursive processing of child plan nodes.

## Definition
static void set_customscan_references(PlannerInfo *root, CustomScan *cscan, int rtoffset)

## Detailed Description
This function processes CustomScan nodes during the plan reference adjustment phase in PostgreSQL's query planner. CustomScan nodes allow extensions to implement custom scan methods. The function handles two distinct reference adjustment scenarios:

1. **Custom scan tuple handling**: When the CustomScan has a custom scan target list (custom_scan_tlist) or operates without a specific scan relation (scanrelid == 0), it uses fix_upper_expr() to adjust references to point to the custom scan tuple output.

2. **Standard scan handling**: When using standard relation scanning, it uses fix_scan_list() to adjust references in the conventional manner.

Additionally, this function recursively processes any child plan nodes within the CustomScan's custom_plans list, ensuring that nested plan structures maintain proper variable references.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The CustomScan plan node whose references need to be adjusted
- : Range table offset to be applied to relation IDs and variable references

## Dependencies
- Functions called/Symbols referenced:
  - [build_tlist_index](../b/build_tlist_index.md)
  - [fix_upper_expr](../f/fix_upper_expr.md)
  - fix_scan_list
  - [set_plan_refs](set_plan_refs.md)
  - [offset_relid_set](../o/offset_relid_set.md)
  - [pfree](../p/pfree.md)
  - lfirst
  - NUM_EXEC_TLIST
  - NUM_EXEC_QUAL
  - INDEX_VAR
  - NRM_EQUAL
- Called from (representative examples):
  - [set_plan_refs](set_plan_refs.md)
  - fix_scan_list

## Notes and Other Information
- This is a static function within setrefs.c for internal plan reference adjustment
- The function supports PostgreSQL's extensibility framework by handling CustomScan nodes created by extensions
- It recursively processes child plans using set_plan_refs(), allowing for complex nested custom scan structures
- Custom expression lists (custom_exprs) are properly adjusted along with standard plan elements
- The custom_relids set is offset to maintain proper relation references
- Similar structure to set_foreignscan_references but includes child plan processing capability

## Simplified Source

```c
static void
set_customscan_references(PlannerInfo *root, CustomScan *cscan, int rtoffset) {
    // Adjust scan relation ID if valid
    if (cscan->scan.scanrelid > 0) {
        cscan->scan.scanrelid += rtoffset;
    }

    if (cscan->custom_scan_tlist != NIL || cscan->scan.scanrelid == 0) {
        // Custom scan tuple handling: build index and use fix_upper_expr
        indexed_tlist *itlist = build_tlist_index(cscan->custom_scan_tlist);

        // Fix target list, quals, and custom expressions to reference custom tuple
        cscan->scan.plan.targetlist = fix_upper_expr(root, cscan->scan.plan.targetlist,
                                                     itlist, INDEX_VAR, rtoffset);
        cscan->scan.plan.qual = fix_upper_expr(root, cscan->scan.plan.qual,
                                              itlist, INDEX_VAR, rtoffset);
        cscan->custom_exprs = fix_upper_expr(root, cscan->custom_exprs,
                                           itlist, INDEX_VAR, rtoffset);

        pfree(itlist);
        cscan->custom_scan_tlist = fix_scan_list(root, cscan->custom_scan_tlist, rtoffset);
    } else {
        // Standard scan handling: use fix_scan_list
        cscan->scan.plan.targetlist = fix_scan_list(root, cscan->scan.plan.targetlist, rtoffset);
        cscan->scan.plan.qual = fix_scan_list(root, cscan->scan.plan.qual, rtoffset);
        cscan->custom_exprs = fix_scan_list(root, cscan->custom_exprs, rtoffset);
    }

    // Process child plans recursively
    foreach(lc, cscan->custom_plans) {
        lfirst(lc) = set_plan_refs(root, (Plan *) lfirst(lc), rtoffset);
    }

    // Adjust relation ID sets
    cscan->custom_relids = offset_relid_set(cscan->custom_relids, rtoffset);
}
```