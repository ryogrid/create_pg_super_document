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