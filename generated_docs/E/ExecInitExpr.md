# ExecInitExpr

## Location
[src/backend/executor/execExpr.c:135-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L135-L171)

## Overview
ExecInitExpr prepares an expression tree for execution by building and returning an ExprState that implements the given expression node tree for subsequent evaluation.

## Definition

```c
ExprState *
ExecInitExpr(Expr *node, PlanState *parent)
```
## Detailed Description
ExecInitExpr is a core function in PostgreSQL's expression evaluation system that transforms a read-only expression tree (Expr node) into an executable state machine (ExprState). The function builds a series of evaluation steps that can be efficiently executed by ExecEvalExpr. The resulting ExprState contains compiled bytecode-like instructions that eliminate the overhead of recursive tree traversal during expression evaluation.

The function operates in several phases:
1. Creates an empty ExprState node and initializes basic fields
2. Inserts any necessary setup steps via ExecCreateExprSetupSteps
3. Recursively compiles the expression tree into evaluation steps via ExecInitExprRec
4. Appends a final DONE step to mark the end of execution
5. Prepares the expression for execution via ExecReadyExpr

The compilation process supports concurrent execution of the same plan tree since the original Expr tree remains read-only, though individual ExprState instances cannot be shared between concurrent executions due to runtime mutation.

## Parameters / Member Variables
- : The root of the expression tree to compile (Expr*). If NULL, returns NULL for caller convenience.
- : The PlanState node that owns this expression. May be NULL for expressions not associated with a plan tree (though such expressions cannot contain aggregates or subplans).

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new ExprState)
  - [ExecCreateExprSetupSteps](ExecCreateExprSetupSteps.md) (inserts setup steps)
  - [ExecInitExprRec](ExecInitExprRec.md) (recursively compiles expression)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (adds evaluation steps)
  - [ExecReadyExpr](ExecReadyExpr.md) (finalizes expression for execution)
  - EEOP_DONE (opcode constant)
- Called from (representative examples):
  - [ExecInitCheck](ExecInitCheck.md) (for check constraints)
  - [ExecInitExprList](ExecInitExprList.md) (for expression lists)
  - [ExecPrepareExpr](ExecPrepareExpr.md) (for standalone expressions)
  - Various node initialization functions (ExecInitLimit, ExecInitMemoize, etc.)

## Notes and Other Information
- Must be called in a memory context that persists for the lifetime of expression executions
- Automatically handles Aggref, WindowFunc, and SubPlan nodes by adding them to parent PlanState lists
- No corresponding ExecEndExpr function exists; cleanup occurs via memory context release
- Functions requiring additional cleanup can register shutdown callbacks in the ExprContext
- Returns NULL for NULL input expressions; NULL ExprState pointers are accepted by ExecQual and ExecCheck but not ExecEvalExpr
- The compiled ExprState uses a step-based execution model for optimal performance