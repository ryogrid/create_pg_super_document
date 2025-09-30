# plan_create_index_workers

## Location
[src/backend/optimizer/plan/planner.c:6859-6992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6859-L6992)

## Overview
Uses the planner to determine the optimal number of parallel worker processes for CREATE INDEX operations based on table characteristics and system constraints.

## Definition

```c
int
plan_create_index_workers(Oid tableOid, Oid indexOid)
```
## Detailed Description
The  function performs intelligent sizing of parallel worker processes for index creation and rebuilding operations. It evaluates multiple factors to determine the optimal level of parallelism:

1. **Safety checks**: Verifies that parallel operations are allowed (not standalone backend, parallelism enabled)
2. **Parallel safety analysis**: Ensures the table is not temporary and that index expressions/predicates are parallel-safe
3. **Explicit configuration**: Honors table-level  storage parameter if set
4. **Automatic sizing**: Uses  to estimate workers based on heap size
5. **Memory constraints**: Caps workers to ensure each gets adequate memory (minimum 32MB per participant)

The function considers both btree and BRIN indexes, which support parallel builds. It ensures each tuplesort participant (including the leader) receives sufficient memory allocation from the total  budget.

## Parameters / Member Variables
- : Object ID of the table on which the index will be built
- : Object ID of the index to be created or reindexed (must support parallel builds)

## Dependencies
- Functions called/Symbols referenced:
  - ,  - Planner infrastructure types
  -  - [Node](../N/Node.md) creation utility
  -  - Sets up relation arrays
  -  - Creates RelOptInfo structure
  - ,  - [Relation](../R/Relation.md) access functions
  - ,  - Index metadata access
  -  - Parallel safety analysis
  -  - Estimates table size parameters
  -  - Generic parallel worker computation
  - ,  - [Relation](../R/Relation.md) cleanup
- Called from (representative examples):
  -  - During index construction process

## Notes and Other Information
- Returns 0 if parallel operation is unsafe or not beneficial
- Caller must hold appropriate locks on both table and index
- Only supports indexes with parallel build capability (btree, BRIN)
- Uses inheritance marking (inh=true) to prevent index info fetching during REINDEX
- Considers memory distribution carefully - each participant needs minimum 32MB
- Respects  global limit
- Automatic fallback to 0 workers for temporary tables or non-parallel-safe expressions
- Leader process participates as worker but is not counted in return value
- Memory calculation includes leader process in total participant count
- Safe to proceed when return value is > 0, may be unsafe when 0

## Simplified Source

```c
int
plan_create_index_workers(Oid tableOid, Oid indexOid)
{
    PlannerInfo *root;
    Query *query;
    PlannerGlobal *glob;
    RangeTblEntry *rte;
    Relation heap;
    Relation index;
    RelOptInfo *rel;
    int parallel_workers;
    BlockNumber heap_blocks;
    double reltuples;
    double allvisfrac;

    // Check if parallel operations are allowed
    if (!IsUnderPostmaster || max_parallel_maintenance_workers == 0)
        return 0;

    // Set up minimal planner state for analysis
    query = makeNode(Query);
    query->commandType = CMD_SELECT;

    glob = makeNode(PlannerGlobal);

    root = makeNode(PlannerInfo);
    root->parse = query;
    root->glob = glob;
    root->query_level = 1;
    root->planner_cxt = CurrentMemoryContext;
    root->wt_param_id = -1;
    root->join_domains = list_make1(makeNode(JoinDomain));

    // Build minimal RTE (marked with inh=true to prevent index info fetching)
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = tableOid;
    rte->relkind = RELKIND_RELATION;
    rte->rellockmode = AccessShareLock;
    rte->lateral = false;
    rte->inh = true;
    rte->inFromCl = true;
    query->rtable = list_make1(rte);
    addRTEPermissionInfo(&query->rteperminfos, rte);

    // Set up relation structures
    setup_simple_rel_arrays(root);
    rel = build_simple_rel(root, 1, NULL);

    // Open relations (caller already has locks)
    heap = table_open(tableOid, NoLock);
    index = index_open(indexOid, NoLock);

    // Check safety: no temp tables, parallel-safe expressions/predicates
    if (heap->rd_rel->relpersistence == RELPERSISTENCE_TEMP ||
        !is_parallel_safe(root, (Node *) RelationGetIndexExpressions(index)) ||
        !is_parallel_safe(root, (Node *) RelationGetIndexPredicate(index))) {
        parallel_workers = 0;
        goto done;
    }

    // Honor explicit parallel_workers table setting if specified
    if (rel->rel_parallel_workers != -1) {
        parallel_workers = Min(rel->rel_parallel_workers,
                              max_parallel_maintenance_workers);
        goto done;
    }

    // Estimate heap size and compute workers based on table size
    estimate_rel_size(heap, NULL, &heap_blocks, &reltuples, &allvisfrac);
    parallel_workers = compute_parallel_worker(rel, heap_blocks, -1,
                                              max_parallel_maintenance_workers);

    // Cap workers based on memory constraints (minimum 32MB per participant)
    while (parallel_workers > 0 &&
           maintenance_work_mem / (parallel_workers + 1) < 32768L)
        parallel_workers--;

done:
    index_close(index, NoLock);
    table_close(heap, NoLock);

    return parallel_workers;
}
```