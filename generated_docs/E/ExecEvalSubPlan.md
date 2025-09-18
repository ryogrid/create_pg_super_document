# ExecEvalSubPlan

## Location
[src/backend/executor/execExprInterp.c:4753-4769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4753-L4769)

## Overview
ExecEvalSubPlan serves as a bridge function in the expression interpreter that delegates subplan evaluation to the specialized subplan execution infrastructure in nodeSubplan.c.

## Definition
void ExecEvalSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)

## Detailed Description
This function acts as an interface between PostgreSQL's expression interpreter (execExprInterp.c) and the dedicated subplan execution module (nodeSubplan.c). When the expression interpreter encounters a subplan operation during expression evaluation, it calls this function to hand off the actual subplan execution to the specialized ExecSubPlan function.

The function performs minimal setup work - it extracts the SubPlanState from the operation descriptor, performs a stack depth check to prevent stack overflow in potentially nested subplan scenarios, and then delegates to ExecSubPlan for the actual execution. This design maintains clean separation between the general expression evaluation machinery and the specialized subplan handling logic.

Subplans are typically used for subqueries, EXISTS clauses, IN/NOT IN operations, and similar constructs that require executing a separate query plan within the context of a larger expression evaluation.

## Parameters / Member Variables
- : The ExprState containing the expression evaluation context
- : The ExprEvalStep operation descriptor containing the subplan SubPlanState and result storage pointers  
- : The ExprContext providing the evaluation context for the subplan execution

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [ExecSubPlan](ExecSubPlan.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (main expression interpreter loop)

## Notes and Other Information
- This function is a thin wrapper that maintains architectural separation between expression interpretation and subplan execution
- The stack depth check is crucial because subplans can be deeply nested, potentially leading to stack overflow
- The actual subplan logic is implemented in nodeSubplan.c, keeping the expression interpreter focused on its core responsibilities
- Subplans are a fundamental mechanism for executing correlated and uncorrelated subqueries in PostgreSQL
- The function works with the broader subplan infrastructure including SubPlanState management and caching