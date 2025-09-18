# ExecInitExprRec

## Location
[src/backend/executor/execExpr.c:894-2601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L894-L2601)

## Overview
ExecInitExprRec is the core recursive function that compiles expression nodes into execution steps, generating a sequence of operations stored in an ExprState for efficient runtime evaluation.

## Definition
static void ExecInitExprRec(Expr *node, ExprState *state, Datum *resv, bool *resnull)

## Detailed Description
ExecInitExprRec is PostgreSQL's central expression compilation function that transforms abstract syntax tree nodes (Expr) into executable step sequences. This function performs a large switch statement over all possible expression node types (Var, Const, Param, FuncExpr, OpExpr, etc.) and generates corresponding evaluation steps that are appended to the ExprState's steps array. The function uses recursion to handle nested expressions and implements optimizations like short-circuiting for boolean operations, specialized handling for scalar array operations with hashing, and efficient memory management through proper context switching. Each generated step contains an opcode and associated data needed for runtime evaluation.

## Parameters / Member Variables
- node: The expression node to be compiled into execution steps
- state: The ExprState structure where generated steps will be appended
- resv: Pointer to where the expression result value should be stored
- resnull: Pointer to where the expression result null flag should be stored

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalPushStep](ExprEvalPushStep.md) (primary step creation function)
  - check_stack_depth (prevents stack overflow)
  - [ExecInitFunc](ExecInitFunc.md) (for function expressions)
  - [ExecInitSubPlan](ExecInitSubPlan.md) (for subplan expressions)
  - [ExecInitExprRec](ExecInitExprRec.md) (recursive calls for nested expressions)
  - [ExecReadyExpr](ExecReadyExpr.md) (for array coercion subexpressions)
  - Various opcode constants (EEOP_VAR, EEOP_CONST, etc.)
- Called from (representative examples):
  - [ExecInitExpr](ExecInitExpr.md)
  - [ExecInitExprWithParams](ExecInitExprWithParams.md)  
  - [ExecInitQual](ExecInitQual.md)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - Recursive calls from within itself

## Notes and Other Information
- This is a static function spanning over 1700 lines covering all PostgreSQL expression types
- Implements stack depth checking to prevent infinite recursion on malformed expressions
- Handles complex expression types including aggregates, window functions, array operations, JSON expressions, and domain coercions
- Uses specialized opcodes for performance optimization (e.g., EEOP_HASHED_SCALARARRAYOP for large IN clauses)
- Manages memory contexts carefully to ensure proper allocation lifetime
- The function directly modifies the ExprState by appending evaluation steps rather than returning values
- Critical for PostgreSQL's expression evaluation performance as it determines the execution plan for all expressions