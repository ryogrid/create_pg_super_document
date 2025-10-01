# ExecInitFunctionScan

## Location
[src/backend/executor/nodeFunctionscan.c:279-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L279-L529)

## Overview
ExecInitFunctionScan initializes a FunctionScanState node for executing function scans, setting up tuple descriptors, expression contexts, and per-function state structures.

## Definition

```c
FunctionScanState *
ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitFunctionScan performs comprehensive initialization for function scan operations:

1. **State Structure Setup**: Creates and configures a FunctionScanState with proper executor integration, including setting the ExecProcNode to ExecFunctionScan.

2. **Optimization Detection**: Determines if this is a "simple" scan (single function, no ordinality) for performance optimization.

3. **Per-Function Initialization**: For each function in the scan:
   - Sets up table function result expressions via ExecInitTableFunctionResult
   - Builds appropriate tuple descriptors based on function return types
   - Handles composite types, scalar types, and explicit column definitions
   - Creates individual function slots when needed

4. **Tuple Descriptor Construction**: Creates the combined scan tuple descriptor by:
   - Copying from the single function's descriptor in simple cases
   - Building a composite descriptor from all functions in complex cases
   - Adding an ordinality column if requested

5. **Slot and Context Setup**: Initializes scan tuple slots, result projection, and creates a dedicated memory context for function argument evaluation.

The function handles various complexity levels from simple single-function scans to complex multi-function scans with ordinality columns.

## Parameters / Member Variables
- : FunctionScan plan node containing the function list and configuration
- : EState providing the executor state and memory contexts
- : Execution flags controlling scan behavior (EXEC_FLAG_BACKWARD supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecInitTableFunctionResult](ExecInitTableFunctionResult.md)
  - [BuildDescFromLists](../B/BuildDescFromLists.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - [get_expr_result_type](../g/get_expr_result_type.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md)
  - [TupleDescCopyEntry](../T/TupleDescCopyEntry.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - AllocSetContextCreate
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Supports both simple (single function) and complex (multiple functions/ordinality) scan modes
- Handles composite, scalar, and record function return types
- Creates dedicated argument evaluation context to avoid memory leaks
- Validates that function scans have no child nodes
- Properly initializes ordinality tracking even when not outputting ordinality columns
- Integrates with PostgreSQL's standard executor initialization patterns

## Simplified Source

```c
FunctionScanState *
ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags)
{
    // Create and initialize basic state structure
    FunctionScanState *scanstate = makeNode(FunctionScanState);
    scanstate->ss.ps.plan = (Plan *) node;
    scanstate->ss.ps.state = estate;
    scanstate->ss.ps.ExecProcNode = ExecFunctionScan;
    scanstate->eflags = eflags;

    // Set up basic scan parameters
    int nfuncs = list_length(node->functions);
    scanstate->ordinality = node->funcordinality;
    scanstate->nfuncs = nfuncs;
    scanstate->simple = (nfuncs == 1 && !node->funcordinality);
    scanstate->ordinal = 0;

    // Create expression context
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    // Initialize per-function state
    scanstate->funcstates = palloc(nfuncs * sizeof(FunctionScanPerFuncState));
    int natts = 0;
    int i = 0;
    ListCell *lc;

    foreach(lc, node->functions)
    {
        RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);
        FunctionScanPerFuncState *fs = &scanstate->funcstates[i];

        // Initialize function expression
        fs->setexpr = ExecInitTableFunctionResult((Expr *) rtfunc->funcexpr,
                                                 scanstate->ss.ps.ps_ExprContext,
                                                 &scanstate->ss.ps);
        fs->tstore = NULL;
        fs->rowcount = -1;

        // Build tuple descriptor for this function
        TupleDesc tupdesc;
        if (rtfunc->funccolnames != NIL)
        {
            tupdesc = BuildDescFromLists(rtfunc->funccolnames,
                                        rtfunc->funccoltypes,
                                        rtfunc->funccoltypmods,
                                        rtfunc->funccolcollations);
            BlessTupleDesc(tupdesc);
        }
        else
        {
            TypeFuncClass functypclass;
            Oid funcrettype;
            functypclass = get_expr_result_type(rtfunc->funcexpr, &funcrettype, &tupdesc);

            if (functypclass == TYPEFUNC_COMPOSITE || functypclass == TYPEFUNC_COMPOSITE_DOMAIN)
            {
                tupdesc = CreateTupleDescCopy(tupdesc);
            }
            else if (functypclass == TYPEFUNC_SCALAR)
            {
                tupdesc = CreateTemplateTupleDesc(1);
                TupleDescInitEntry(tupdesc, 1, NULL, funcrettype, -1, 0);
                TupleDescInitEntryCollation(tupdesc, 1, exprCollation(rtfunc->funcexpr));
            }
        }

        fs->tupdesc = tupdesc;
        fs->colcount = rtfunc->funccolcount;

        // Create function slot if needed
        if (!scanstate->simple)
            fs->func_slot = ExecInitExtraTupleSlot(estate, fs->tupdesc, &TTSOpsMinimalTuple);
        else
            fs->func_slot = NULL;

        natts += rtfunc->funccolcount;
        i++;
    }

    // Create the combined scan tuple descriptor
    TupleDesc scan_tupdesc;
    if (scanstate->simple)
    {
        scan_tupdesc = CreateTupleDescCopy(scanstate->funcstates[0].tupdesc);
        scan_tupdesc->tdtypeid = RECORDOID;
        scan_tupdesc->tdtypmod = -1;
    }
    else
    {
        if (node->funcordinality)
            natts++;

        scan_tupdesc = CreateTemplateTupleDesc(natts);
        AttrNumber attno = 0;

        // Copy function columns
        for (i = 0; i < nfuncs; i++)
        {
            TupleDesc tupdesc = scanstate->funcstates[i].tupdesc;
            int colcount = scanstate->funcstates[i].colcount;
            for (int j = 1; j <= colcount; j++)
                TupleDescCopyEntry(scan_tupdesc, ++attno, tupdesc, j);
        }

        // Add ordinality column if needed
        if (node->funcordinality)
            TupleDescInitEntry(scan_tupdesc, ++attno, NULL, INT8OID, -1, 0);
    }

    // Initialize scan slot, result type, and projection
    ExecInitScanTupleSlot(estate, &scanstate->ss, scan_tupdesc, &TTSOpsMinimalTuple);
    ExecInitResultTypeTL(&scanstate->ss.ps);
    ExecAssignScanProjectionInfo(&scanstate->ss);

    // Initialize qualification expressions
    scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

    // Create argument evaluation context
    scanstate->argcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                 "Table function arguments",
                                                 ALLOCSET_DEFAULT_SIZES);

    return scanstate;
}
```