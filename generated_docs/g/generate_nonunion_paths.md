# generate_nonunion_paths

## Location
[src/backend/optimizer/prep/prepunion.c:1018-1207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1018-L1207)

## Overview
Generates execution paths for INTERSECT, INTERSECT ALL, EXCEPT, and EXCEPT ALL operations by recursively processing left and right operands and choosing optimal hash or sort-based strategies.

## Definition

```c
static RelOptInfo *
generate_nonunion_paths(SetOperationStmt *op, PlannerInfo *root,
						List *refnames_tlist,
						List **pTargetList)
```
## Detailed Description
This function handles path generation for set operations that require comparing two input sets (INTERSECT and EXCEPT operations). It follows a systematic approach:

1. **Child Processing**: Forces tuple_fraction to 0.0 to ensure children fetch all tuples, then recursively processes left and right operands using 
2. **Input Ordering**: Determines optimal input order - for EXCEPT, left input must be first; for INTERSECT, smaller input (fewer groups) is placed first to minimize hash table size
3. **Path Construction**: Creates an Append path combining both child paths, with proper target list generation including a flag column for set operation processing
4. **Strategy Selection**: Uses  to decide between hash-based or sort-based execution strategy based on estimated costs and data characteristics
5. **Final Path Creation**: Adds appropriate sort path if needed, then creates the final SetOp path node with the chosen strategy and proper SetOpCmd

The function handles both ALL and non-ALL variants of INTERSECT and EXCEPT operations, with different row estimation strategies for each.

## Parameters / Member Variables
- `*op`: SetOperationStmt containing the operation type (INTERSECT/EXCEPT), ALL flag, column types and collations
- `*root`: PlannerInfo providing global planning context and configuration settings
- `*refnames_tlist`: List of reference names for constructing the target list
- `**pTargetList`: Output parameter returning the generated target list for the operation
## Dependencies
- Functions called/Symbols referenced:
  - [recurse_set_operations](../r/recurse_set_operations.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)
  - [generate_append_tlist](generate_append_tlist.md)
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - create_pathtarget
  - [create_append_path](../c/create_append_path.md)
  - [generate_setop_grouplist](generate_setop_grouplist.md)
  - [choose_hashed_setop](../c/choose_hashed_setop.md)
  - [create_sort_path](../c/create_sort_path.md)
  - [make_pathkeys_for_sortclauses](../m/make_pathkeys_for_sortclauses.md)
  - [create_setop_path](../c/create_setop_path.md)
- Called from (representative examples):
  - [recurse_set_operations](../r/recurse_set_operations.md)

## Notes and Other Information
- The function temporarily sets root->tuple_fraction to 0.0 to ensure complete data retrieval from children
- For EXCEPT operations, the left input order is mandatory; for INTERSECT, input order is optimized based on size
- Row estimates are conservative worst-case calculations: non-ALL cases estimate one output row per group, ALL cases use relevant relation size
- The generated target list includes a special flag column that must appear as a variable (not constant) to avoid confusion in later planning phases
- [Hash](../H/Hash.md) vs. sort strategy selection considers factors like data size, available memory, and cost estimates

## Simplified Source

```c
static RelOptInfo *
generate_nonunion_paths(SetOperationStmt *op, PlannerInfo *root,
                        List *refnames_tlist, List **pTargetList)
{
    RelOptInfo *result_rel, *lrel, *rrel;
    Path *lpath, *rpath, *path;
    List *lpath_tlist, *rpath_tlist, *tlist_list, *tlist, *groupList, *pathlist;
    double dLeftGroups, dRightGroups, dNumGroups, dNumOutputRows;
    bool use_hash;
    SetOpCmd cmd;
    int firstFlag;

    // Save and reset tuple fraction to fetch all tuples from children
    double save_fraction = root->tuple_fraction;
    root->tuple_fraction = 0.0;

    // Process left operand
    lrel = recurse_set_operations(op->larg, root, op->colTypes, op->colCollations,
                                  false, 0, refnames_tlist, &lpath_tlist, NULL);
    if (lrel->rtekind == RTE_SUBQUERY)
        build_setop_child_paths(root, lrel, true, lpath_tlist, NIL, &dLeftGroups);
    else
        dLeftGroups = lrel->rows;
    lpath = lrel->cheapest_total_path;

    // Process right operand
    rrel = recurse_set_operations(op->rarg, root, op->colTypes, op->colCollations,
                                  false, 1, refnames_tlist, &rpath_tlist, NULL);
    if (rrel->rtekind == RTE_SUBQUERY)
        build_setop_child_paths(root, rrel, true, rpath_tlist, NIL, &dRightGroups);
    else
        dRightGroups = rrel->rows;
    rpath = rrel->cheapest_total_path;

    // Restore tuple fraction
    root->tuple_fraction = save_fraction;

    // Order inputs: EXCEPT requires left first, INTERSECT prefers smaller first
    if (op->op == SETOP_EXCEPT || dLeftGroups <= dRightGroups) {
        pathlist = list_make2(lpath, rpath);
        tlist_list = list_make2(lpath_tlist, rpath_tlist);
        firstFlag = 0;
    } else {
        pathlist = list_make2(rpath, lpath);
        tlist_list = list_make2(rpath_tlist, lpath_tlist);
        firstFlag = 1;
    }

    // Generate target list and create result relation
    tlist = generate_append_tlist(op->colTypes, op->colCollations, true,
                                  tlist_list, refnames_tlist);
    *pTargetList = tlist;

    result_rel = fetch_upper_rel(root, UPPERREL_SETOP,
                                 bms_union(lrel->relids, rrel->relids));
    result_rel->reltarget = create_pathtarget(root, tlist);

    // Create append path combining both inputs
    path = (Path *) create_append_path(root, result_rel, pathlist, NIL, NIL, NULL, 0, false, -1);

    // Generate grouping information
    groupList = generate_setop_grouplist(op, tlist);

    // Estimate output characteristics
    if (op->op == SETOP_EXCEPT) {
        dNumGroups = dLeftGroups;
        dNumOutputRows = op->all ? lpath->rows : dNumGroups;
    } else {
        dNumGroups = Min(dLeftGroups, dRightGroups);
        dNumOutputRows = op->all ? Min(lpath->rows, rpath->rows) : dNumGroups;
    }

    // Choose hash vs sort strategy
    use_hash = choose_hashed_setop(root, groupList, path, dNumGroups, dNumOutputRows,
                                   (op->op == SETOP_INTERSECT) ? "INTERSECT" : "EXCEPT");

    // Add sort if needed
    if (groupList && !use_hash)
        path = (Path *) create_sort_path(root, result_rel, path,
                                         make_pathkeys_for_sortclauses(root, groupList, tlist), -1.0);

    // Create final SetOp path
    switch (op->op) {
        case SETOP_INTERSECT:
            cmd = op->all ? SETOPCMD_INTERSECT_ALL : SETOPCMD_INTERSECT;
            break;
        case SETOP_EXCEPT:
            cmd = op->all ? SETOPCMD_EXCEPT_ALL : SETOPCMD_EXCEPT;
            break;
        default:
            elog(ERROR, "unrecognized set op: %d", (int) op->op);
            cmd = SETOPCMD_INTERSECT;
            break;
    }

    path = (Path *) create_setop_path(root, result_rel, path, cmd,
                                      use_hash ? SETOP_HASHED : SETOP_SORTED,
                                      groupList, list_length(op->colTypes) + 1,
                                      use_hash ? firstFlag : -1,
                                      dNumGroups, dNumOutputRows);

    result_rel->rows = path->rows;
    add_path(result_rel, path);
    return result_rel;
}
```