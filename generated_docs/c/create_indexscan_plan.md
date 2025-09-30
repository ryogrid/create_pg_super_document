# create_indexscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3006-3201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3006-L3201)

## Overview
Creates an index scan plan node for scanning a base relation using an index, supporting both regular IndexScan and IndexOnlyScan operations.

## Definition
```c
static Scan *
create_indexscan_plan(PlannerInfo *root,
                      IndexPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      bool indexonly)
```

## Detailed Description
The `create_indexscan_plan` function creates either an `IndexScan` or `IndexOnlyScan` plan node depending on the `indexonly` parameter. This function performs complex qualification preprocessing that is common to both scan types. Key operations include:

1. **Index Qualification Processing**: Extracts and processes index qualification expressions, substituting index variables for table variables and handling nested loop parameters.

2. **ORDER BY Processing**: Handles index-based ordering by looking up sort operators for ORDER BY expressions when applicable.

3. **Qualification Filtering**: Determines which scan clauses need to be checked at execution time (qpqual) versus those automatically handled by the index. This includes:
   - Removing pseudoconstant clauses
   - Eliminating clauses redundant with index conditions
   - Checking for clauses implied by index qualifications

4. **Index-Only Scan Optimization**: For index-only scans, marks columns that the index access method cannot return as resjunk to avoid generating references to unavailable columns.

The function supports both forward and backward index scans and handles complex scenarios like parameterized paths and ORDER BY optimization.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context information
- `best_path`: IndexPath representing the chosen index access path with cost estimates and index information
- `tlist`: Target list specifying which columns/expressions should be returned by the scan
- `scan_clauses`: List of RestrictInfo nodes representing WHERE clause conditions
- `indexonly`: Boolean flag indicating whether to create an IndexOnlyScan (true) or regular IndexScan (false)

## Dependencies
- Functions called/Symbols referenced:
  - [fix_indexqual_references](../f/fix_indexqual_references.md)
  - [fix_indexorderby_references](../f/fix_indexorderby_references.md)
  - [is_redundant_with_indexclauses](../i/is_redundant_with_indexclauses.md)
  - [contain_mutable_functions](contain_mutable_functions.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [make_indexonlyscan](../m/make_indexonlyscan.md)
  - [make_indexscan](../m/make_indexscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [IndexPath](../I/IndexPath.md), IndexOptInfo, PathKey (struct types)
  - ForwardScanDirection, BackwardScanDirection (enum values)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)
  - [create_bitmap_subplan](create_bitmap_subplan.md)

## Notes and Other Information
- This function serves a dual purpose, creating both IndexScan and IndexOnlyScan nodes based on the `indexonly` parameter
- Includes sophisticated logic to minimize runtime qualification checking by leveraging index capabilities
- Handles complex cases like OR'd index conditions and parameterized index scans
- The qualification processing logic mirrors `extract_nonindex_conditions()` in costsize.c for consistency
- Index-only scans can provide significant performance benefits when all required columns are available in the index
- Supports both B-tree style ordered scans and unordered index access methods
- The function validates scan direction and ensures proper handling of outer-relation variables in nested loops

## Simplified Source

```c
static Scan *
create_indexscan_plan(PlannerInfo *root, IndexPath *best_path, List *tlist,
                     List *scan_clauses, bool indexonly) {
    List *indexclauses = best_path->indexclauses;
    List *indexorderbys = best_path->indexorderbys;
    Index baserelid = best_path->path.parent->relid;
    IndexOptInfo *indexinfo = best_path->indexinfo;
    Oid indexoid = indexinfo->indexoid;
    List *qpqual, *stripped_indexquals, *fixed_indexquals, *fixed_indexorderbys;
    List *indexorderbyops = NIL;

    // Validate this is a base relation with valid scan direction
    Assert(baserelid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);
    Assert(best_path->indexscandir == ForwardScanDirection ||
           best_path->indexscandir == BackwardScanDirection);

    // Process index qualifications - substitute index vars for table vars
    fix_indexqual_references(root, best_path, &stripped_indexquals, &fixed_indexquals);

    // Process ORDER BY expressions for index ordering
    fixed_indexorderbys = fix_indexorderby_references(root, best_path);

    // Determine which scan clauses need runtime checking (qpqual)
    // Exclude clauses handled automatically by the index
    qpqual = NIL;
    foreach(l, scan_clauses) {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);

        // Skip pseudoconstants, redundant clauses, and implied conditions
        if (rinfo->pseudoconstant)
            continue;
        if (is_redundant_with_indexclauses(rinfo, indexclauses))
            continue;
        if (!contain_mutable_functions((Node *) rinfo->clause) &&
            predicate_implied_by(list_make1(rinfo->clause), stripped_indexquals, false))
            continue;

        qpqual = lappend(qpqual, rinfo);
    }

    // Optimize qualification order and extract clauses
    qpqual = order_qual_clauses(root, qpqual);
    qpqual = extract_actual_clauses(qpqual, false);

    // Handle nestloop parameter replacement
    if (best_path->path.param_info) {
        stripped_indexquals = (List *) replace_nestloop_params(root, (Node *) stripped_indexquals);
        qpqual = (List *) replace_nestloop_params(root, (Node *) qpqual);
        indexorderbys = (List *) replace_nestloop_params(root, (Node *) indexorderbys);
    }

    // Process ORDER BY expressions - look up sort operators
    if (indexorderbys) {
        forboth(pathkeyCell, best_path->path.pathkeys, exprCell, indexorderbys) {
            PathKey *pathkey = lfirst(pathkeyCell);
            Node *expr = lfirst(exprCell);
            Oid exprtype = exprType(expr);

            // Get sort operator from opfamily
            Oid sortop = get_opfamily_member(pathkey->pk_opfamily, exprtype,
                                           exprtype, pathkey->pk_strategy);
            if (!OidIsValid(sortop))
                elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
                     pathkey->pk_strategy, exprtype, exprtype, pathkey->pk_opfamily);
            indexorderbyops = lappend_oid(indexorderbyops, sortop);
        }
    }

    // For index-only scans, mark unreturnable columns as resjunk
    if (indexonly) {
        int i = 0;
        foreach(l, indexinfo->indextlist) {
            TargetEntry *indextle = lfirst(l);
            indextle->resjunk = !indexinfo->canreturn[i];
            i++;
        }
    }

    // Create the appropriate scan plan node
    Scan *scan_plan;
    if (indexonly) {
        scan_plan = (Scan *) make_indexonlyscan(tlist, qpqual, baserelid, indexoid,
                                               fixed_indexquals, stripped_indexquals,
                                               fixed_indexorderbys, indexinfo->indextlist,
                                               best_path->indexscandir);
    } else {
        scan_plan = (Scan *) make_indexscan(tlist, qpqual, baserelid, indexoid,
                                           fixed_indexquals, stripped_indexquals,
                                           fixed_indexorderbys, indexorderbys,
                                           indexorderbyops, best_path->indexscandir);
    }

    copy_generic_path_info(&scan_plan->plan, &best_path->path);
    return scan_plan;
}
```