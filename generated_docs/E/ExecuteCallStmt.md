# ExecuteCallStmt

## Location
[src/backend/commands/functioncmds.c:2188-2364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L2188-L2364)

## Overview
Executes CALL statements to invoke stored procedures, handling transaction control, parameter evaluation, and result processing in both atomic and non-atomic execution contexts.

## Definition
```c
void ExecuteCallStmt(CallStmt *stmt, ParamListInfo params, bool atomic, DestReceiver *dest)
```

## Detailed Description
This function implements PostgreSQL's CALL statement functionality for executing stored procedures. It handles the complex semantics of procedure calls, particularly around transaction control in non-atomic contexts where procedures can execute COMMIT/ROLLBACK statements. The function creates a CallContext that tracks the atomic/non-atomic execution context, evaluates procedure arguments, performs security checks, and invokes the procedure.

The function manages several critical aspects: permission checking (ACL_EXECUTE), argument evaluation within an appropriate execution context, snapshot management for non-atomic contexts, and result handling for procedures that return records. It enforces restrictions on transaction control based on procedure properties like security definer and proconfig settings.

## Parameters / Member Variables
- `stmt`: CallStmt node containing the parsed CALL statement with function expression and arguments
- `params`: ParamListInfo containing parameter values for prepared statements
- `atomic`: Boolean controlling transaction behavior - false allows transaction commands within the procedure
- `dest`: DestReceiver for sending procedure results back to the client

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md) (permission verification)
  - [CreateExecutorState](../C/CreateExecutorState.md)/CreateExprContext (execution environment)
  - [ExecPrepareExpr](ExecPrepareExpr.md)/ExecEvalExprSwitchContext (argument evaluation)
  - FunctionCallInvoke (procedure execution)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)/end_tup_output (result handling)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)/PushActiveSnapshot (snapshot management)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Establishes non-atomic execution context when atomic=false, allowing transaction commands
- Forces atomic=true for security definer procedures and procedures with proconfig
- Handles RECORD-type return values by sending tuples directly to the destination
- Creates CallContext node passed to procedure via fcinfo->context
- Manages snapshots carefully in non-atomic contexts due to potential COMMIT/ROLLBACK
- Includes TOAST pointer safety considerations for procedures that do transaction control
- Part of PostgreSQL's stored procedure infrastructure supporting SQL standard semantics
- Supports nested CALL statements with proper context tracking

## Simplified Source

```c
void ExecuteCallStmt(CallStmt *stmt, ParamListInfo params, bool atomic, DestReceiver *dest)
{
    LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
    FuncExpr *fexpr = stmt->funcexpr;
    CallContext *callcontext;
    EState *estate;
    ExprContext *econtext;
    HeapTuple tp;
    FmgrInfo flinfo;
    Datum retval;
    int nargs, i;
    ListCell *lc;

    // Check execute permission on the procedure
    AclResult aclresult = object_aclcheck(ProcedureRelationId, fexpr->funcid,
                                         GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_PROCEDURE, get_func_name(fexpr->funcid));

    // Create context object for procedure execution
    callcontext = makeNode(CallContext);
    callcontext->atomic = atomic;

    // Look up procedure metadata
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for function %u", fexpr->funcid);

    // Force atomic mode for certain procedure types
    if (!heap_attisnull(tp, Anum_pg_proc_proconfig, NULL))
        callcontext->atomic = true;  // Procedures with proconfig can't do transaction control

    if (((Form_pg_proc) GETSTRUCT(tp))->prosecdef)
        callcontext->atomic = true;  // Security definer procedures can't do transaction control

    ReleaseSysCache(tp);

    // Validate argument count
    nargs = list_length(fexpr->args);
    if (nargs > FUNC_MAX_ARGS)
        ereport(ERROR, /* too many arguments */);

    // Initialize function call info
    InvokeFunctionExecuteHook(fexpr->funcid);
    fmgr_info(fexpr->funcid, &flinfo);
    fmgr_info_set_expr((Node *) fexpr, &flinfo);
    InitFunctionCallInfoData(*fcinfo, &flinfo, nargs, fexpr->inputcollid,
                            (Node *) callcontext, NULL);

    // Create execution context for argument evaluation
    estate = CreateExecutorState();
    estate->es_param_list_info = params;
    econtext = CreateExprContext(estate);

    // Get current snapshot for non-atomic contexts
    if (!atomic)
        PushActiveSnapshot(GetTransactionSnapshot());

    // Evaluate all procedure arguments
    i = 0;
    foreach(lc, fexpr->args)
    {
        ExprState *exprstate = ExecPrepareExpr(lfirst(lc), estate);
        bool isnull;
        Datum val = ExecEvalExprSwitchContext(exprstate, econtext, &isnull);

        fcinfo->args[i].value = val;
        fcinfo->args[i].isnull = isnull;
        i++;
    }

    // Clean up argument evaluation snapshot
    if (!atomic)
        PopActiveSnapshot();

    // Execute the procedure
    PgStat_FunctionCallUsage fcusage;
    pgstat_init_function_usage(fcinfo, &fcusage);
    retval = FunctionCallInvoke(fcinfo);
    pgstat_end_function_usage(&fcusage, true);

    // Handle procedure return value
    if (fexpr->funcresulttype == VOIDOID)
    {
        // Void procedure - no result to process
    }
    else if (fexpr->funcresulttype == RECORDOID)
    {
        // Return record tuple to client
        if (fcinfo->isnull)
            elog(ERROR, "procedure returned null record");

        EnsurePortalSnapshotExists();

        HeapTupleHeader td = DatumGetHeapTupleHeader(retval);
        Oid tupType = HeapTupleHeaderGetTypeId(td);
        int32 tupTypmod = HeapTupleHeaderGetTypMod(td);
        TupleDesc retdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

        // Send tuple to destination
        TupOutputState *tstate = begin_tup_output_tupdesc(dest, retdesc, &TTSOpsHeapTuple);

        HeapTupleData rettupdata;
        rettupdata.t_len = HeapTupleHeaderGetDatumLength(td);
        ItemPointerSetInvalid(&(rettupdata.t_self));
        rettupdata.t_tableOid = InvalidOid;
        rettupdata.t_data = td;

        TupleTableSlot *slot = ExecStoreHeapTuple(&rettupdata, tstate->slot, false);
        tstate->dest->receiveSlot(slot, tstate->dest);

        end_tup_output(tstate);
        ReleaseTupleDesc(retdesc);
    }
    else
        elog(ERROR, "unexpected result type for procedure: %u", fexpr->funcresulttype);

    // Cleanup execution state
    FreeExecutorState(estate);
}
```