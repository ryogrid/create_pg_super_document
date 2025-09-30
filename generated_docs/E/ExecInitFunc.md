# ExecInitFunc

## Location
[src/backend/executor/execExpr.c:2628-2732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L2628-L2732)

## Overview
Performs setup necessary for the evaluation of function-like expressions by appending argument evaluation steps to the expression state and preparing function call structures for efficient runtime execution.

## Definition

```c
structure too */
	InitFunctionCallInfoData(*fcinfo, flinfo,
							 nargs, inputcollid, NULL, NULL);
```
## Detailed Description
ExecInitFunc is a critical internal function in PostgreSQL's expression evaluation system that initializes function call expressions during query plan setup. It handles all the preparatory work needed to execute function calls efficiently at runtime, including permission checks, argument setup, and opcode selection based on function characteristics.

The function performs several key operations:
1. **Security validation**: Checks ACL permissions to ensure the user can execute the specified function
2. **Function metadata setup**: Initializes FmgrInfo and FunctionCallInfo structures with function details
3. **Argument processing**: Handles both constant and variable arguments, optimizing constants by pre-evaluating them
4. **Opcode selection**: Chooses the appropriate execution opcode based on function strictness and statistics tracking requirements

The function is designed to be called during expression compilation and prepares a "scratch" ExprEvalStep that can be customized by callers before being pushed to the execution steps list.

## Parameters / Member Variables
- : Pre-allocated ExprEvalStep structure to be populated with function call setup data
- : The original Expr node representing the function call (used for error reporting and metadata)
- : List of argument expressions to be evaluated before function execution
- : OID of the function to be called, used for permission checks and function lookup
- : Collation ID to be used for the function call, affects string operations
- : Current ExprState containing the expression compilation context and step list

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md) (permission validation)
  - [fmgr_info](../f/fmgr_info.md) (function manager setup)
  - InitFunctionCallInfoData (function call structure initialization)
  - [ExecInitExprRec](ExecInitExprRec.md) (recursive argument expression setup)
  - [executor_errposition](../e/executor_errposition.md) (error position reporting)
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md) (multiple call sites for different function expression types)

## Notes and Other Information
- This function does not push the prepared step to the execution list, allowing callers to modify the opcode for special cases like DISTINCT operations
- Includes safety checks for maximum argument count (FUNC_MAX_ARGS) and rejects set-returning functions in scalar contexts
- Optimizes constant arguments by pre-evaluating them during setup rather than at every execution
- Selects different opcodes based on function strictness and statistics tracking level to optimize runtime performance
- The function is static and only used internally within the expression evaluation system

## Simplified Source

```c
static void ExecInitFunc(ExprEvalStep *scratch, Expr *node, List *args, Oid funcid,
                        Oid inputcollid, ExprState *state)
{
    int nargs = list_length(args);
    AclResult aclresult;
    FmgrInfo *flinfo;
    FunctionCallInfo fcinfo;
    int argno;
    ListCell *arg_cell;

    // Check permission to execute function
    aclresult = object_aclcheck(ProcedureRelationId, funcid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(funcid));
    InvokeFunctionExecuteHook(funcid);

    // Validate argument count
    if (nargs > FUNC_MAX_ARGS)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg_plural("cannot pass more than %d argument to a function",
                              "cannot pass more than %d arguments to a function",
                              FUNC_MAX_ARGS, FUNC_MAX_ARGS)));

    // Allocate function call structures
    scratch->d.func.finfo = palloc0(sizeof(FmgrInfo));
    scratch->d.func.fcinfo_data = palloc0(SizeForFunctionCallInfo(nargs));
    flinfo = scratch->d.func.finfo;
    fcinfo = scratch->d.func.fcinfo_data;

    // Set up function manager info
    fmgr_info(funcid, flinfo);
    fmgr_info_set_expr((Node *) node, flinfo);

    // Initialize function call parameter structure
    InitFunctionCallInfoData(*fcinfo, flinfo, nargs, inputcollid, NULL, NULL);

    // Cache function address and argument count for runtime efficiency
    scratch->d.func.fn_addr = flinfo->fn_addr;
    scratch->d.func.nargs = nargs;

    // Reject set-returning functions in scalar context
    if (flinfo->fn_retset)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set"),
                 state->parent ?
                 executor_errposition(state->parent->state, exprLocation((Node *) node)) : 0));

    // Initialize arguments: optimize constants, evaluate variables at runtime
    argno = 0;
    foreach(arg_cell, args) {
        Expr *arg = (Expr *) lfirst(arg_cell);

        if (IsA(arg, Const)) {
            // Pre-evaluate constant arguments for efficiency
            Const *const_arg = (Const *) arg;
            fcinfo->args[argno].value = const_arg->constvalue;
            fcinfo->args[argno].isnull = const_arg->constisnull;
        } else {
            // Set up runtime evaluation for variable arguments
            ExecInitExprRec(arg, state,
                           &fcinfo->args[argno].value,
                           &fcinfo->args[argno].isnull);
        }
        argno++;
    }

    // Choose opcode based on function characteristics and statistics tracking
    if (pgstat_track_functions <= flinfo->fn_stats) {
        scratch->opcode = (flinfo->fn_strict && nargs > 0) ?
                         EEOP_FUNCEXPR_STRICT : EEOP_FUNCEXPR;
    } else {
        scratch->opcode = (flinfo->fn_strict && nargs > 0) ?
                         EEOP_FUNCEXPR_STRICT_FUSAGE : EEOP_FUNCEXPR_FUSAGE;
    }
}
```