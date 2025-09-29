# ExecInitExprWithParams

## Location
[src/backend/executor/execExpr.c:172-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L172-L220)

## Overview
ExecInitExprWithParams prepares a standalone expression tree for execution, similar to ExecInitExpr but designed for expressions not associated with a plan tree and supporting external parameters.

## Definition

```c
ExprState *
ExecInitExprWithParams(Expr *node, ParamListInfo ext_params)
```
## Detailed Description
ExecInitExprWithParams is a variant of ExecInitExpr specifically designed for standalone expression compilation. The key difference is that it operates without a parent PlanState and instead accepts a ParamListInfo structure to handle PARAM_EXTERN parameters. This makes it suitable for evaluating expressions outside the context of a regular query plan execution.

The function follows the same compilation process as ExecInitExpr:
1. Creates an empty ExprState node with parent set to NULL
2. Sets the ext_params field to the provided ParamListInfo
3. Inserts setup steps via ExecCreateExprSetupSteps
4. Recursively compiles the expression tree via ExecInitExprRec
5. Appends a DONE step and prepares the expression for execution

This function is typically used for expressions that need to be evaluated independently of query execution, such as in partition pruning contexts where expressions may need to be evaluated with external parameter values.

## Parameters / Member Variables
- : The root of the expression tree to compile (Expr*). Returns NULL if NULL is passed for caller convenience.
- : A ParamListInfo structure describing external parameters (PARAM_EXTERN) that may be referenced in the expression. Can be NULL if no external parameters are needed.

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new ExprState)
  - [ExecCreateExprSetupSteps](ExecCreateExprSetupSteps.md) (inserts setup steps)
  - [ExecInitExprRec](ExecInitExprRec.md) (recursively compiles expression)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (adds evaluation steps)
  - [ExecReadyExpr](ExecReadyExpr.md) (finalizes expression for execution)
  - EEOP_DONE (opcode constant)
  - [ParamListInfo](../P/ParamListInfo.md) (parameter information structure)
- Called from (representative examples):
  - [InitPartitionPruneContext](../I/InitPartitionPruneContext.md) (for partition pruning expressions)
  - [ExecProcNode](ExecProcNode.md) (header inclusion)

## Notes and Other Information
- Designed specifically for standalone expressions not associated with a plan tree
- Cannot handle expressions containing aggregates or subplans since there's no parent PlanState
- The ext_params field allows the expression to access external parameters during evaluation
- Shares the same memory context requirements as ExecInitExpr - must be called in a context that persists for the expression's lifetime
- Like ExecInitExpr, returns NULL for NULL input and requires no explicit cleanup function
- Primarily used in specialized contexts like partition pruning where expressions need evaluation outside normal query execution

## Simplified Source

```c
ExprState *
ExecInitExprWithParams(Expr *node, ParamListInfo ext_params)
{
    // Handle NULL expression - return NULL for caller convenience
    if (node == NULL)
        return NULL;

    // Initialize empty ExprState for standalone evaluation
    ExprState *state = makeNode(ExprState);
    ExprEvalStep step = {0};

    // Set properties for standalone expression (no parent PlanState)
    state->expr = node;
    state->parent = NULL;          // No parent since this is standalone
    state->ext_params = ext_params; // External parameters for PARAM_EXTERN nodes

    // Insert any required setup steps for parameters, etc.
    ExecCreateExprSetupSteps(state, (Node *) node);

    // Recursively compile the expression tree into evaluation steps
    ExecInitExprRec(node, state, &state->resvalue, &state->resnull);

    // Add final DONE step
    step.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &step);

    // Finalize for execution
    ExecReadyExpr(state);

    return state;
}
```