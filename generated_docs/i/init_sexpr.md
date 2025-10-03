# init_sexpr

## Location
[src/backend/executor/execSRF.c:696-809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L696-L809)

## Overview
Initializes a SetExprState node during first use by setting up function metadata, permission checks, call information, and result descriptors for both regular and set-returning functions.

## Definition
```c
static void init_sexpr(Oid foid, Oid input_collation, Expr *node, SetExprState *sexpr, PlanState *parent, MemoryContext sexprCxt, bool allowSRF, bool needDescForSRF)
```

## Detailed Description
init_sexpr is a critical initialization function that prepares a SetExprState for function execution. It performs several essential setup tasks:

1. **Security and Validation**: Checks ACL permissions for function execution and validates argument counts against FUNC_MAX_ARGS
2. **Function Manager Setup**: Initializes fmgr structures using fmgr_info_cxt and creates FunctionCallInfo with proper parameter counts and collation
3. **Set-Returning Function Validation**: Verifies that SRF usage is allowed in the current context and ensures consistency between declared and actual function behavior
4. **Result Descriptor Creation**: For SRFs requiring result descriptors, analyzes the function's return type and creates appropriate TupleDesc structures:
   - **TYPEFUNC_COMPOSITE/COMPOSITE_DOMAIN**: Copies existing composite type descriptors
   - **TYPEFUNC_SCALAR**: Creates single-column descriptors for scalar returns
   - **TYPEFUNC_RECORD**: Handles dynamic record types (may leave descriptor NULL if not needed)

The function handles memory management carefully, allocating long-lived structures in the provided sexprCxt while performing temporary operations in the current context.

## Parameters / Member Variables
- `foid`: OID of the function to initialize
- `input_collation`: Collation to use for the function call
- `node`: Original expression node for error reporting and metadata
- `sexpr`: SetExprState structure to initialize
- `parent`: Parent PlanState for error position reporting
- `sexprCxt`: Memory context for long-lived function state
- `allowSRF`: Whether set-returning functions are permitted in this context  
- `needDescForSRF`: Whether to create result descriptors for set-returning functions

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error (permission checking)
  - [get_func_name](../g/get_func_name.md), InvokeFunctionExecuteHook (function metadata and hooks)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md), fmgr_info_set_expr (function manager setup)
  - SizeForFunctionCallInfo, InitFunctionCallInfoData (call info initialization)
  - [get_expr_result_type](../g/get_expr_result_type.md) (return type analysis)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md), CreateTemplateTupleDesc, TupleDescInitEntry (descriptor creation)
  - [executor_errposition](../e/executor_errposition.md), exprLocation (error reporting)
- Called from (representative examples):
  - [ExecInitTableFunctionResult](../E/ExecInitTableFunctionResult.md) (src/backend/executor/execSRF.c:81)
  - [ExecInitFunctionResultSet](../E/ExecInitFunctionResultSet.md) (src/backend/executor/execSRF.c:462, 470)

## Notes and Other Information
- Static function used internally by execSRF.c initialization routines
- Performs comprehensive validation including security checks and argument count limits
- Creates different descriptor types based on function return characteristics
- The needDescForSRF parameter is crucial for target list SRFs which require result descriptors
- Handles memory context switching to ensure descriptors are allocated in the correct long-lived context
- For RECORD-returning functions, may leave funcResultDesc as NULL if the function can determine its own result structure
- Initializes additional state fields (funcResultStore, funcResultSlot, shutdown_reg) to safe defaults
- The allowSRF parameter enables context-sensitive validation of set-returning function usage

## Simplified Source

```c
static void
init_sexpr(Oid foid, Oid input_collation, Expr *node,
           SetExprState *sexpr, PlanState *parent,
           MemoryContext sexprCxt, bool allowSRF, bool needDescForSRF)
{
    size_t numargs = list_length(sexpr->args);

    // Security check: verify permission to execute function
    AclResult aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
    InvokeFunctionExecuteHook(foid);

    // Validate argument count doesn't exceed limits
    if (list_length(sexpr->args) > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                        errmsg_plural("cannot pass more than %d argument to a function",
                                     "cannot pass more than %d arguments to a function",
                                     FUNC_MAX_ARGS, FUNC_MAX_ARGS)));

    // Set up function manager lookup information
    fmgr_info_cxt(foid, &(sexpr->func), sexprCxt);
    fmgr_info_set_expr((Node *) sexpr->expr, &(sexpr->func));

    // Initialize function call parameter structure
    sexpr->fcinfo = (FunctionCallInfo) palloc(SizeForFunctionCallInfo(numargs));
    InitFunctionCallInfoData(*sexpr->fcinfo, &(sexpr->func), numargs,
                             input_collation, NULL, NULL);

    // Check if set-returning function usage is allowed
    if (sexpr->func.fn_retset && !allowSRF)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set"),
                        parent ? executor_errposition(parent->state, exprLocation((Node *) node)) : 0));

    Assert(sexpr->func.fn_retset == sexpr->funcReturnsSet);

    // Create result descriptor for set-returning functions if needed
    if (sexpr->func.fn_retset && needDescForSRF) {
        TypeFuncClass functypclass;
        Oid funcrettype;
        TupleDesc tupdesc;

        functypclass = get_expr_result_type(sexpr->func.fn_expr, &funcrettype, &tupdesc);

        MemoryContext oldcontext = MemoryContextSwitchTo(sexprCxt);

        if (functypclass == TYPEFUNC_COMPOSITE || functypclass == TYPEFUNC_COMPOSITE_DOMAIN) {
            // Composite data type - copy existing descriptor
            Assert(tupdesc);
            sexpr->funcResultDesc = CreateTupleDescCopy(tupdesc);
            sexpr->funcReturnsTuple = true;
        } else if (functypclass == TYPEFUNC_SCALAR) {
            // Scalar type - create single-column descriptor
            tupdesc = CreateTemplateTupleDesc(1);
            TupleDescInitEntry(tupdesc, (AttrNumber) 1, NULL, funcrettype, -1, 0);
            sexpr->funcResultDesc = tupdesc;
            sexpr->funcReturnsTuple = false;
        } else if (functypclass == TYPEFUNC_RECORD) {
            // Record type - may work without descriptor
            sexpr->funcResultDesc = NULL;
            sexpr->funcReturnsTuple = true;
        } else {
            // Other types - will fail if descriptor needed
            sexpr->funcResultDesc = NULL;
        }

        MemoryContextSwitchTo(oldcontext);
    } else {
        sexpr->funcResultDesc = NULL;
    }

    // Initialize additional state to safe defaults
    sexpr->funcResultStore = NULL;
    sexpr->funcResultSlot = NULL;
    sexpr->shutdown_reg = false;
}
```