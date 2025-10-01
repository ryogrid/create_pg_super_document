# ExecInitIndexScan

## Location
[src/backend/executor/nodeIndexscan.c:886-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L886-L1134)

## Overview
ExecInitIndexScan initializes the execution state for an index scan node, setting up scan keys, opening relations, and preparing all necessary data structures for index scanning operations.

## Definition

```c
structure
	 */
	indexstate = makeNode(IndexScanState);
```
## Detailed Description
ExecInitIndexScan is the initialization function for index scan execution nodes. It creates and configures an IndexScanState structure containing all the information needed to execute index scans. The function handles both the base relation and index relation setup, as index scans require tracking two separate relations.

The function performs several key operations:
1. Creates and initializes the IndexScanState structure
2. Opens the base relation being scanned
3. Initializes tuple slot and result type information
4. Processes index qualification expressions and ORDER BY expressions
5. Opens the index relation
6. Builds scan keys from index qualifications using ExecIndexBuildScanKeys
7. Sets up ORDER BY processing including sort support if needed
8. Creates runtime expression context for evaluating runtime keys

The function includes special handling for EXPLAIN-only execution, where it stops early to allow index advisor plugins to explain plans with non-existent indexes. It also properly handles runtime keys that need evaluation during scan execution.

## Parameters / Member Variables
- : Pointer to IndexScan plan node containing the index scan specification
- : Execution state containing global execution context and parameters
- : Execution flags controlling initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecInitExprList](ExecInitExprList.md)
  - [exec_rt_fetch](../e/exec_rt_fetch.md)
  - [index_open](../i/index_open.md)
  - [ExecIndexBuildScanKeys](ExecIndexBuildScanKeys.md)
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [pairingheap_allocate](../p/pairingheap_allocate.md)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (in execProcnode.c:220)

## Notes and Other Information
- The function handles two relations: the base table being scanned and the index being used
- Runtime keys require a separate expression context that is not reset for every tuple
- ORDER BY expressions are processed similarly to index qualifications but require additional sort support setup
- The reorder queue is initialized for handling ORDER BY expression re-checking when needed
- Special EXPLAIN-only mode allows index advisor plugins to work with non-existent indexes
- The function properly manages memory contexts for different types of expression evaluation
- Located in src/backend/executor/nodeIndexscan.c:886-1134

## Simplified Source

```c
IndexScanState *
ExecInitIndexScan(IndexScan *node, EState *estate, int eflags)
{
    IndexScanState *indexstate;
    Relation currentRelation;
    LOCKMODE lockmode;

    // Create and initialize state structure
    indexstate = makeNode(IndexScanState);
    indexstate->ss.ps.plan = (Plan *) node;
    indexstate->ss.ps.state = estate;
    indexstate->ss.ps.ExecProcNode = ExecIndexScan;

    // Set up expression context
    ExecAssignExprContext(estate, &indexstate->ss.ps);

    // Open the base relation
    currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    indexstate->ss.ss_currentRelation = currentRelation;
    indexstate->ss.ss_currentScanDesc = NULL; // No heap scan here

    // Initialize scan tuple slot using table descriptor
    ExecInitScanTupleSlot(estate, &indexstate->ss,
                         RelationGetDescr(currentRelation),
                         table_slot_callbacks(currentRelation));

    // Initialize result type and projection
    ExecInitResultTypeTL(&indexstate->ss.ps);
    ExecAssignScanProjectionInfo(&indexstate->ss);

    // Initialize qualification expressions
    indexstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) indexstate);
    indexstate->indexqualorig = ExecInitQual(node->indexqualorig, (PlanState *) indexstate);
    indexstate->indexorderbyorig = ExecInitExprList(node->indexorderbyorig, (PlanState *) indexstate);

    // Early exit for EXPLAIN-only operations
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return indexstate;

    // Open the index relation
    lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
    indexstate->iss_RelationDesc = index_open(node->indexid, lockmode);

    // Initialize scan state
    indexstate->iss_RuntimeKeysReady = false;
    indexstate->iss_RuntimeKeys = NULL;
    indexstate->iss_NumRuntimeKeys = 0;

    // Build scan keys from index qualifications
    ExecIndexBuildScanKeys((PlanState *) indexstate,
                          indexstate->iss_RelationDesc,
                          node->indexqual, false,
                          &indexstate->iss_ScanKeys,
                          &indexstate->iss_NumScanKeys,
                          &indexstate->iss_RuntimeKeys,
                          &indexstate->iss_NumRuntimeKeys,
                          NULL, NULL);

    // Build ORDER BY scan keys
    ExecIndexBuildScanKeys((PlanState *) indexstate,
                          indexstate->iss_RelationDesc,
                          node->indexorderby, true,
                          &indexstate->iss_OrderByKeys,
                          &indexstate->iss_NumOrderByKeys,
                          &indexstate->iss_RuntimeKeys,
                          &indexstate->iss_NumRuntimeKeys,
                          NULL, NULL);

    // Initialize sort support for ORDER BY expressions if needed
    if (indexstate->iss_NumOrderByKeys > 0)
    {
        int numOrderByKeys = indexstate->iss_NumOrderByKeys;
        ListCell *lco, *lcx;
        int i = 0;

        // Allocate sort support structures
        indexstate->iss_SortSupport = (SortSupportData *)
            palloc0(numOrderByKeys * sizeof(SortSupportData));
        indexstate->iss_OrderByTypByVals = (bool *)
            palloc(numOrderByKeys * sizeof(bool));
        indexstate->iss_OrderByTypLens = (int16 *)
            palloc(numOrderByKeys * sizeof(int16));

        // Setup sort support for each ORDER BY expression
        forboth(lco, node->indexorderbyops, lcx, node->indexorderbyorig)
        {
            Oid orderbyop = lfirst_oid(lco);
            Node *orderbyexpr = (Node *) lfirst(lcx);
            Oid orderbyType = exprType(orderbyexpr);
            Oid orderbyColl = exprCollation(orderbyexpr);
            SortSupport orderbysort = &indexstate->iss_SortSupport[i];

            // Configure sort support
            orderbysort->ssup_cxt = CurrentMemoryContext;
            orderbysort->ssup_collation = orderbyColl;
            orderbysort->ssup_nulls_first = false;
            orderbysort->ssup_attno = 0;
            orderbysort->abbreviate = false;
            PrepareSortSupportFromOrderingOp(orderbyop, orderbysort);

            get_typlenbyval(orderbyType,
                           &indexstate->iss_OrderByTypLens[i],
                           &indexstate->iss_OrderByTypByVals[i]);
            i++;
        }

        // Allocate arrays for recalculated distances and initialize reorder queue
        indexstate->iss_OrderByValues = (Datum *)
            palloc(numOrderByKeys * sizeof(Datum));
        indexstate->iss_OrderByNulls = (bool *)
            palloc(numOrderByKeys * sizeof(bool));
        indexstate->iss_ReorderQueue = pairingheap_allocate(reorderqueue_cmp, indexstate);
    }

    // Set up runtime context for runtime keys if needed
    if (indexstate->iss_NumRuntimeKeys != 0)
    {
        ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;
        ExecAssignExprContext(estate, &indexstate->ss.ps);
        indexstate->iss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
        indexstate->ss.ps.ps_ExprContext = stdecontext;
    }
    else
    {
        indexstate->iss_RuntimeContext = NULL;
    }

    return indexstate;
}
```