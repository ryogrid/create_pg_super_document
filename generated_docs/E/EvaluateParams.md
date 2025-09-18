# EvaluateParams

## Location
src/backend/commands/prepare.c: 278 - 368

## Overview
Evaluates a list of parameter expressions for a prepared statement, performing type checking, coercion, and expression evaluation to produce a ParamListInfo structure for query execution.

## Definition


## Detailed Description
EvaluateParams processes parameter expressions provided to an EXECUTE statement by validating parameter count, performing parse analysis and type transformation on each parameter expression, coercing parameter values to expected types, and evaluating the expressions to produce concrete parameter values. The function ensures type safety by checking that provided parameters can be coerced to the types expected by the prepared statement, and produces a ParamListInfo structure that can be used during query execution.

## Parameters / Member Variables
- : Parse state containing parsing context and source text information
- : PreparedStatement containing parameter type information and metadata
- : List of parameter expressions (raw parser output) provided in the EXECUTE statement
- : Executor state used for expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (creates copy of parameter expressions)
  - [transformExpr](../t/transformExpr.md) (performs parse analysis on expressions)
  - exprType (determines expression result type)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (performs type coercion)
  - [assign_expr_collations](../a/assign_expr_collations.md) (handles collation assignment)
  - [ExecPrepareExprList](ExecPrepareExprList.md) (prepares expressions for execution)
  - [makeParamList](../m/makeParamList.md) (creates ParamListInfo structure)
  - ExecEvalExprSwitchContext (evaluates expressions)
  - GetPerTupleExprContext (gets evaluation context)
- Called from (representative examples):
  - [ExecuteQuery](ExecuteQuery.md) (parameter evaluation for EXECUTE statements)
  - [ExplainExecuteQuery](ExplainExecuteQuery.md) (parameter evaluation for EXPLAIN EXECUTE)

## Notes and Other Information
- Validates that the number of provided parameters matches the expected count from the prepared statement
- Returns NULL when no parameters are required, optimizing the common case
- Performs type coercion using assignment semantics, allowing implicit casts where appropriate
- Copies parameter expressions before transformation to avoid modifying parser input
- Evaluates parameters in per-tuple expression context for proper memory management
- Marks all parameters with PARAM_FLAG_CONST since they represent constant values
- Provides detailed error messages for parameter count mismatches and type coercion failures
- Handles collation assignment for string-type parameters to ensure correct comparison semantics