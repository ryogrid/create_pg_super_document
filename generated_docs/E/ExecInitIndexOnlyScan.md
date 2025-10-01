# ExecInitIndexOnlyScan

## Location
[src/backend/executor/nodeIndexonlyscan.c:506-705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L506-L705)

## Overview
ExecInitIndexOnlyScan initializes the execution state for an index-only scan node, setting up scan keys, opening relations, and configuring all necessary data structures for index-only scan operations.

## Definition
```c
IndexOnlyScanState *ExecInitIndexOnlyScan(IndexOnlyScan *node, EState *estate, int eflags)
```

## Detailed Description
This function performs comprehensive initialization for index-only scan operations, which are a critical optimization in PostgreSQL that allows retrieving data directly from index pages without accessing the heap table when all required columns are available in the index.

The initialization process involves multiple phases:

1. **State Structure Creation**: Creates and initializes the IndexOnlyScanState node with proper executor framework integration
2. **Expression Context Setup**: Establishes expression evaluation contexts for runtime computations
3. **Relation Management**: Opens both the base relation and index relation with appropriate lock modes
4. **Tuple Descriptor Setup**: Creates tuple descriptors based on the index target list rather than physical index structure
5. **Slot Allocation**: Allocates tuple slots for both index tuples and table tuples (needed for visibility rechecking)
6. **Projection Setup**: Configures result type and projection information with INDEX_VAR variable references
7. **Qualification Setup**: Initializes both scan qualifications and recheck qualifications
8. **Scan Key Construction**: Builds scan keys from index qualifications and ORDER BY expressions
9. **Runtime Key Handling**: Sets up separate expression context for runtime key evaluation
10. **Name Type Optimization**: Detects and handles the special case where btree indexes store cstrings for name types

The function includes sophisticated handling of the "name" data type optimization where btree indexes store cstrings instead of full name values for efficiency, requiring special conversion logic during tuple storage.

## Parameters / Member Variables
- `node`: Pointer to the IndexOnlyScan plan node containing scan specifications and target information
- `estate`: Execution state containing transaction context, tuple tables, and other execution resources
- `eflags`: Execution flags that control initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY for plan explanation)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md)
  - [ExecTypeFromTL](ExecTypeFromTL.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecAllocTableSlot](ExecAllocTableSlot.md)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfoWithVarno](ExecAssignScanProjectionInfoWithVarno.md)
  - [ExecInitQual](ExecInitQual.md)
  - [exec_rt_fetch](../e/exec_rt_fetch.md)
  - [index_open](../i/index_open.md)
  - [ExecIndexBuildScanKeys](ExecIndexBuildScanKeys.md)
- Types used:
  - [IndexOnlyScan](../I/IndexOnlyScan.md)
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [EState](EState.md)
  - [TupleDesc](../T/TupleDesc.md)
  - AttrNumber
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- The function distinguishes between regular scans and EXPLAIN-only operations, stopping early for the latter
- Index-only scans maintain two tuple slots: one for index data and one for table data (used during visibility rechecking)
- Runtime keys require a separate expression context to avoid conflicts with per-tuple context resets
- The name type optimization is a btree-specific performance enhancement that may be adopted by other index access methods
- Proper lock mode management ensures consistency with the overall transaction isolation level
- The function handles both index qualification and ORDER BY expressions as scan keys
- Memory allocation uses palloc for PostgreSQL's memory context management

## Simplified Source

```c
IndexOnlyScanState *
ExecInitIndexOnlyScan(IndexOnlyScan *node, EState *estate, int eflags)
{
    IndexOnlyScanState *indexstate;
    Relation currentRelation;
    Relation indexRelation;
    LOCKMODE lockmode;
    TupleDesc tupDesc;

    // Create and initialize state structure
    indexstate = makeNode(IndexOnlyScanState);
    indexstate->ss.ps.plan = (Plan *) node;
    indexstate->ss.ps.state = estate;
    indexstate->ss.ps.ExecProcNode = ExecIndexOnlyScan;

    // Set up expression context
    ExecAssignExprContext(estate, &indexstate->ss.ps);

    // Open the base relation being scanned
    currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    indexstate->ss.ss_currentRelation = currentRelation;
    indexstate->ss.ss_currentScanDesc = NULL; // No heap scan needed

    // Build tuple descriptor from index target list (not physical index descriptor)
    tupDesc = ExecTypeFromTL(node->indextlist);
    ExecInitScanTupleSlot(estate, &indexstate->ss, tupDesc, &TTSOpsVirtual);

    // Create additional table slot for visibility rechecking
    indexstate->ioss_TableSlot =
        ExecAllocTableSlot(&estate->es_tupleTable,
                          RelationGetDescr(currentRelation),
                          table_slot_callbacks(currentRelation));

    // Initialize result type and projection (INDEX_VAR references)
    ExecInitResultTypeTL(&indexstate->ss.ps);
    ExecAssignScanProjectionInfoWithVarno(&indexstate->ss, INDEX_VAR);

    // Initialize qualification expressions
    indexstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) indexstate);
    indexstate->recheckqual = ExecInitQual(node->recheckqual, (PlanState *) indexstate);

    // Early exit for EXPLAIN-only operations
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return indexstate;

    // Open the index relation
    lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
    indexRelation = index_open(node->indexid, lockmode);
    indexstate->ioss_RelationDesc = indexRelation;

    // Initialize scan state
    indexstate->ioss_RuntimeKeysReady = false;
    indexstate->ioss_RuntimeKeys = NULL;
    indexstate->ioss_NumRuntimeKeys = 0;

    // Build scan keys from index qualifications
    ExecIndexBuildScanKeys((PlanState *) indexstate, indexRelation,
                          node->indexqual, false,
                          &indexstate->ioss_ScanKeys,
                          &indexstate->ioss_NumScanKeys,
                          &indexstate->ioss_RuntimeKeys,
                          &indexstate->ioss_NumRuntimeKeys,
                          NULL, NULL);

    // Build ORDER BY scan keys
    ExecIndexBuildScanKeys((PlanState *) indexstate, indexRelation,
                          node->indexorderby, true,
                          &indexstate->ioss_OrderByKeys,
                          &indexstate->ioss_NumOrderByKeys,
                          &indexstate->ioss_RuntimeKeys,
                          &indexstate->ioss_NumRuntimeKeys,
                          NULL, NULL);

    // Set up runtime context for runtime keys if needed
    if (indexstate->ioss_NumRuntimeKeys != 0)
    {
        ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;
        ExecAssignExprContext(estate, &indexstate->ss.ps);
        indexstate->ioss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
        indexstate->ss.ps.ps_ExprContext = stdecontext;
    }
    else
    {
        indexstate->ioss_RuntimeContext = NULL;
    }

    // Handle name type optimization (cstring storage in btree indexes)
    int namecount = 0;
    int indnkeyatts = indexRelation->rd_index->indnkeyatts;

    // Count name attributes stored as cstrings
    for (int attnum = 0; attnum < indnkeyatts; attnum++)
    {
        if (indexRelation->rd_att->attrs[attnum].atttypid == CSTRINGOID &&
            indexRelation->rd_opcintype[attnum] == NAMEOID)
            namecount++;
    }

    // Create array for name attribute numbers if needed
    if (namecount > 0)
    {
        indexstate->ioss_NameCStringAttNums = (AttrNumber *)
            palloc(sizeof(AttrNumber) * namecount);

        int idx = 0;
        for (int attnum = 0; attnum < indnkeyatts; attnum++)
        {
            if (indexRelation->rd_att->attrs[attnum].atttypid == CSTRINGOID &&
                indexRelation->rd_opcintype[attnum] == NAMEOID)
                indexstate->ioss_NameCStringAttNums[idx++] = (AttrNumber) attnum;
        }
    }
    else
    {
        indexstate->ioss_NameCStringAttNums = NULL;
    }

    indexstate->ioss_NameCStringCount = namecount;

    return indexstate;
}
```