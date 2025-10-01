# ExecInitTableFuncScan

## Location
[src/backend/executor/nodeTableFuncscan.c:111-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L111-L219)

## Overview
ExecInitTableFuncScan initializes a TableFuncScanState node for executing table function scans, setting up all necessary data structures, expression contexts, and type conversion information.

## Definition

```c
TableFuncScanState *
ExecInitTableFuncScan(TableFuncScan *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitTableFuncScan performs comprehensive initialization of a table function scan node. It creates a TableFuncScanState structure and initializes all components needed for table function execution, including expression contexts, tuple descriptors, projection information, and function-specific routines.

The function supports both XMLTABLE and JSON_TABLE operations by selecting the appropriate routine based on the function type. It builds a tuple descriptor from the column specifications, initializes all expressions (document, row, column, and passing expressions), and sets up type input functions for data conversion from text to the target column types.

Key initialization steps include creating a per-table memory context, setting up namespace URIs, and preparing function manager info for efficient type conversions during execution.

## Parameters / Member Variables
- : TableFuncScan plan node containing the table function specification
- : Execution state providing the execution environment and memory context
- : Execution flags, with EXEC_FLAG_MARK being explicitly unsupported

## Dependencies
- Functions called/Symbols referenced:
  - [TableFuncScan](../T/TableFuncScan.md), TableFuncScanState, TableFunc (struct types)
  - [ExecTableFuncScan](ExecTableFuncScan.md) (assigned as execution function)
  - [ExecAssignExprContext](ExecAssignExprContext.md), ExecInitScanTupleSlot, ExecInitResultTypeTL
  - [BuildDescFromLists](../B/BuildDescFromLists.md), ExecAssignScanProjectionInfo, ExecInitQual
  - [ExecInitExpr](ExecInitExpr.md), ExecInitExprList (expression initialization)
  - AllocSetContextCreate, getTypeInputInfo, fmgr_info
- Called from:
  - [ExecInitNode](ExecInitNode.md) (main executor initialization)
  - Referenced in nodeTableFuncscan.h header

## Notes and Other Information
- Supports only XMLTABLE and JSON_TABLE function types currently
- Creates a dedicated memory context for per-table operations
- Asserts that table function scans have no child plans (outer/inner)
- Does not support the EXEC_FLAG_MARK execution flag
- Initializes type input functions for all output columns to enable text-to-type conversion
- Sets up separate expression lists for different components (columns, defaults, values, passing parameters)

## Simplified Source

```c
TableFuncScanState *
ExecInitTableFuncScan(TableFuncScan *node, EState *estate, int eflags)
{
    TableFuncScanState *scanstate;
    TableFunc *tf = node->tablefunc;
    TupleDesc tupdesc;
    int i;

    // Basic validation - no mark support, no child plans
    Assert(!(eflags & EXEC_FLAG_MARK));
    Assert(outerPlan(node) == NULL && innerPlan(node) == NULL);

    // Create and initialize scan state
    scanstate = makeNode(TableFuncScanState);
    scanstate->ss.ps.plan = (Plan *) node;
    scanstate->ss.ps.state = estate;
    scanstate->ss.ps.ExecProcNode = ExecTableFuncScan;

    // Set up execution context
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    // Build tuple descriptor from column specifications
    tupdesc = BuildDescFromLists(tf->colnames, tf->coltypes,
                                tf->coltypmods, tf->colcollations);
    ExecInitScanTupleSlot(estate, &scanstate->ss, tupdesc, &TTSOpsMinimalTuple);

    // Initialize result projection
    ExecInitResultTypeTL(&scanstate->ss.ps);
    ExecAssignScanProjectionInfo(&scanstate->ss);

    // Initialize quals and expressions
    scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, &scanstate->ss.ps);

    // Select routine based on function type (XMLTABLE vs JSON_TABLE)
    scanstate->routine = (tf->functype == TFT_XMLTABLE) ?
                        &XmlTableRoutine : &JsonbTableRoutine;

    // Create per-table memory context
    scanstate->perTableCxt = AllocSetContextCreate(CurrentMemoryContext,
                                                  "TableFunc per value context",
                                                  ALLOCSET_DEFAULT_SIZES);

    // Initialize expressions for various components
    scanstate->ns_names = tf->ns_names;
    scanstate->ns_uris = ExecInitExprList(tf->ns_uris, (PlanState *) scanstate);
    scanstate->docexpr = ExecInitExpr((Expr *) tf->docexpr, (PlanState *) scanstate);
    scanstate->rowexpr = ExecInitExpr((Expr *) tf->rowexpr, (PlanState *) scanstate);
    scanstate->colexprs = ExecInitExprList(tf->colexprs, (PlanState *) scanstate);
    scanstate->coldefexprs = ExecInitExprList(tf->coldefexprs, (PlanState *) scanstate);
    scanstate->colvalexprs = ExecInitExprList(tf->colvalexprs, (PlanState *) scanstate);
    scanstate->passingvalexprs = ExecInitExprList(tf->passingvalexprs, (PlanState *) scanstate);

    scanstate->notnulls = tf->notnulls;

    // Set up type input functions for column conversions
    scanstate->in_functions = palloc(sizeof(FmgrInfo) * tupdesc->natts);
    scanstate->typioparams = palloc(sizeof(Oid) * tupdesc->natts);

    for (i = 0; i < tupdesc->natts; i++) {
        Oid in_funcid;
        getTypeInputInfo(TupleDescAttr(tupdesc, i)->atttypid,
                        &in_funcid, &scanstate->typioparams[i]);
        fmgr_info(in_funcid, &scanstate->in_functions[i]);
    }

    return scanstate;
}
```