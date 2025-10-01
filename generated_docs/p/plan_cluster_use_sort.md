# plan_cluster_use_sort

## Location
[src/backend/optimizer/plan/planner.c:6738-6858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6738-L6858)

## Overview
Uses the planner to determine the optimal execution strategy for CLUSTER command by comparing costs of index scan versus sequential scan plus sort.

## Definition

```c
bool
plan_cluster_use_sort(Oid tableOid, Oid indexOid)
```
## Detailed Description
The  function performs cost-based optimization to decide how CLUSTER should implement table reorganization. Given a table and its btree index, it compares two strategies:

1. **Index scan approach**: Scan tuples in index order directly
2. **Sequential scan + sort approach**: Read all tuples sequentially, then sort them

The function creates a minimal planner context and builds cost estimates for both approaches. It considers factors like:
- Index expression evaluation costs (doubled for sort comparisons)
- Sequential scan costs
- Sort operation costs using maintenance_work_mem
- Index scan costs including any index expressions

The function returns true if sorting is cheaper, false if index scanning is more cost-effective.

## Parameters / Member Variables
- : Object ID of the table to be clustered
- : Object ID of the btree index to cluster on (assumed to already be validated as btree)

## Dependencies
- Functions called/Symbols referenced:
  - , , , ,  - Planner data structures
  -  - [Node](../N/Node.md) creation utility
  -  - Sets up relation arrays for planning
  -  - Creates RelOptInfo for the table
  -  - Estimates tuple width
  -  - Evaluates expression costs
  -  - Creates sequential scan path
  -  - Estimates sort operation cost
  -  - Creates index scan path
- Called from (representative examples):
  -  - During CLUSTER command execution

## Notes and Other Information
- Requires caller to hold appropriate locks on the table
- Short-circuits to sorting if  is disabled
- Handles cases where target index is not usable (not reached indcheckxmin horizon, system index being ignored)
- Uses  for sort cost estimation since CLUSTER is a maintenance operation
- Creates minimal planner state rather than full query planning infrastructure
- Index expression costs are doubled in sort comparison because tuplesort re-evaluates expressions
- Considers only btree indexes as input (validated by caller)
- Returns true (use sort) as fallback when index is not available for use

## Simplified Source

```c
bool
plan_cluster_use_sort(Oid tableOid, Oid indexOid)
{
    PlannerInfo *root;
    Query *query;
    RangeTblEntry *rte;
    RelOptInfo *rel;
    IndexOptInfo *indexInfo;
    Path *seqScanPath, seqScanAndSortPath;
    IndexPath *indexScanPath;
    QualCost indexExprCost;
    Cost comparisonCost;

    // Short-circuit if index scans are disabled
    if (!enable_indexscan)
        return true;  // use sort

    // Set up minimal planner state
    query = makeNode(Query);
    query->commandType = CMD_SELECT;
    root = makeNode(PlannerInfo);
    root->parse = query;

    // Build minimal RTE for the relation
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = tableOid;
    query->rtable = list_make1(rte);

    // Set up relation arrays and build RelOptInfo
    setup_simple_rel_arrays(root);
    rel = build_simple_rel(root, 1, NULL);

    // Find the target index in the relation's index list
    indexInfo = NULL;
    foreach(lc, rel->indexlist) {
        indexInfo = lfirst_node(IndexOptInfo, lc);
        if (indexInfo->indexoid == indexOid)
            break;
    }

    // If index not found, use sort as fallback
    if (lc == NULL)
        return true;

    // Set up basic relation statistics
    rel->rows = rel->tuples;
    rel->reltarget->width = get_relation_data_width(tableOid, NULL);
    root->total_table_pages = rel->pages;

    // Calculate index expression evaluation costs (doubled for sort comparisons)
    cost_qual_eval(&indexExprCost, indexInfo->indexprs, root);
    comparisonCost = 2.0 * (indexExprCost.startup + indexExprCost.per_tuple);

    // Estimate cost of sequential scan + sort
    seqScanPath = create_seqscan_path(root, rel, NULL, 0);
    cost_sort(&seqScanAndSortPath, root, NIL,
              seqScanPath->total_cost, rel->tuples, rel->reltarget->width,
              comparisonCost, maintenance_work_mem, -1.0);

    // Estimate cost of index scan
    indexScanPath = create_index_path(root, indexInfo,
                                      NIL, NIL, NIL, NIL,
                                      ForwardScanDirection, false,
                                      NULL, 1.0, false);

    // Return true if sort is cheaper than index scan
    return (seqScanAndSortPath.total_cost < indexScanPath->path.total_cost);
}
```