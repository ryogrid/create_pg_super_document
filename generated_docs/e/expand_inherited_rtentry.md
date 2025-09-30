# expand_inherited_rtentry

## Location
[src/backend/optimizer/util/inherit.c:86-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/inherit.c#L86-L317)

## Overview
Expands a range table entry (RTE) that has inheritance enabled, adding child relations to the query's range table and building necessary planner data structures for inheritance hierarchies and partitioned tables.

## Definition

```c
void
expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
						 RangeTblEntry *rte, Index rti)
```
## Detailed Description
This function handles the expansion of range table entries marked with the "inh" (inheritance) flag. It supports two main scenarios:

1. **RELATION RTEs**: For partitioned tables or traditional inheritance hierarchies, it adds entries for all child tables to the query's range table and builds additional planner structures including RelOptInfos, AppendRelInfos, and PlanRowMarks.

2. **SUBQUERY RTEs**: For UNION ALL groups treated as appendrels, it builds RelOptInfos for existing subqueries by calling expand_appendrel_subquery.

For partitioned tables, the function calls expand_partitioned_rtentry to recursively handle partition expansion. For traditional inheritance, it uses find_all_inheritors to discover all child tables and processes each one through expand_single_inheritance_child.

The function also handles row locking (FOR UPDATE/SHARE) by updating PlanRowMark structures and may add resjunk columns to the target list for row identification purposes.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and query information
- : RelOptInfo for the parent relation being expanded
- : RangeTblEntry that has the inheritance flag set and needs expansion
- : Index of the RTE in the range table

## Dependencies
- Functions called/Symbols referenced:
  - [expand_appendrel_subquery](expand_appendrel_subquery.md)
  - [expand_partitioned_rtentry](expand_partitioned_rtentry.md)
  - [expand_single_inheritance_child](expand_single_inheritance_child.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [get_plan_rowmark](../g/get_plan_rowmark.md)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [build_simple_rel](../b/build_simple_rel.md)
  - [expand_planner_arrays](expand_planner_arrays.md)
  - [makeVar](../m/makeVar.md), makeTargetEntry, makeWholeRowVar
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md)
- Called from (representative examples):
  - [add_other_rels_to_query](../a/add_other_rels_to_query.md)
  - [expand_appendrel_subquery](expand_appendrel_subquery.md)

## Notes and Other Information
- The original RTE represents the entire inheritance set, while generated RTEs represent individual child relations
- For traditional inheritance, the first generated RTE represents the parent table as a simple member (inh=false)
- For partitioned tables, no separate RTE is needed for the parent since it contains no data
- The function handles temporary tables from other backends by silently ignoring them for safety
- Row locking support includes generating appropriate junk columns (ctid, wholerow, tableoid) when needed
- The function assumes appropriate locks have already been obtained by the rewriter for parent relations

## Simplified Source

```c
void expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
                             RangeTblEntry *rte, Index rti) {
    // Handle SUBQUERY RTEs (UNION ALL groups)
    if (rte->rtekind == RTE_SUBQUERY) {
        expand_appendrel_subquery(root, rel, rte, rti);
        return;
    }

    // Must be a RELATION RTE
    Relation parent_rel = table_open(rte->relid, NoLock);
    LOCKMODE lockmode = rte->rellockmode;

    // Handle row locking setup
    PlanRowMark *rowmark = get_plan_rowmark(root->rowMarks, rti);
    if (rowmark) {
        rowmark->isParent = true;
    }

    // Branch on table type
    if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        // Partitioned table: recursively expand partitions
        RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
        expand_partitioned_rtentry(root, rel, rte, rti, parent_rel,
                                  perminfo->updatedCols, rowmark, lockmode);
    } else {
        // Traditional inheritance: find and expand all child tables
        List *child_oids = find_all_inheritors(rte->relid, lockmode, NULL);
        expand_planner_arrays(root, list_length(child_oids));

        // Process each child table
        foreach(lc, child_oids) {
            Oid child_oid = lfirst_oid(lc);
            Relation child_rel = (child_oid != rte->relid) ?
                               table_open(child_oid, NoLock) : parent_rel;

            // Skip temp tables from other backends
            if (child_oid != rte->relid && RELATION_IS_OTHER_TEMP(child_rel)) {
                table_close(child_rel, lockmode);
                continue;
            }

            // Create RTE and planner structures for child
            RangeTblEntry *child_rte;
            Index child_rtindex;
            expand_single_inheritance_child(root, rte, rti, parent_rel,
                                          rowmark, child_rel,
                                          &child_rte, &child_rtindex);

            // Build RelOptInfo for child
            build_simple_rel(root, child_rtindex, rel);

            // Clean up
            if (child_oid != rte->relid)
                table_close(child_rel, NoLock);
        }
    }

    // Add row locking junk columns if needed
    if (rowmark && rowmark->allMarkTypes != old_mark_types) {
        // Add ctid, wholerow, tableoid columns as needed
        // (detailed junk column logic simplified)
        add_rowmark_junk_columns(root, rowmark);
    }

    table_close(parent_rel, NoLock);
}
```