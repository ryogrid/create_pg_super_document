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
  - [check_stack_depth](../c/check_stack_depth.md) (prevents stack overflow)
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

## Simplified Source

```c
static void
ExecInitExprRec(Expr *node, ExprState *state, Datum *resv, bool *resnull)
{
    // Stack depth check to prevent recursion overflow
    check_stack_depth();

    // Handle null nodes
    if (node == NULL)
    {
        // Create a CONST step that returns NULL
        ExprEvalStep scratch = {0};
        scratch.opcode = EEOP_CONST;
        scratch.d.constval.value = (Datum) 0;
        scratch.d.constval.isnull = true;
        scratch.resvalue = resv;
        scratch.resnull = resnull;
        ExprEvalPushStep(state, &scratch);
        return;
    }

    // Main switch statement handling all expression node types
    switch (nodeTag(node))
    {
        case T_Var:
            // Variable reference - create VAR step with slot info
            ExecInitVar((Var *) node, state, resv, resnull);
            break;

        case T_Const:
            // Constant value - create CONST step
            ExecInitConst((Const *) node, state, resv, resnull);
            break;

        case T_Param:
            // Parameter reference - create PARAM step
            ExecInitParam((Param *) node, state, resv, resnull);
            break;

        case T_FuncExpr:
            // Function call - setup function info and argument evaluation
            ExecInitFunc((FuncExpr *) node, state, resv, resnull);
            break;

        case T_OpExpr:
            // Operator expression - handle as function call
            ExecInitOper((OpExpr *) node, state, resv, resnull);
            break;

        case T_BoolExpr:
            // Boolean expression (AND/OR/NOT) - handle short-circuiting
            ExecInitBoolExpr((BoolExpr *) node, state, resv, resnull);
            break;

        case T_SubPlan:
            // Subplan execution - setup subplan state
            ExecInitSubPlan((SubPlan *) node, state, resv, resnull);
            break;

        case T_Aggref:
            // Aggregate reference - create AGG_VAR step
            ExecInitAggref((Aggref *) node, state, resv, resnull);
            break;

        case T_ScalarArrayOpExpr:
            // Scalar array operation - may use hashing for performance
            ExecInitScalarArrayOp((ScalarArrayOpExpr *) node, state, resv, resnull);
            break;

        // ... many more cases for other expression types ...
        // CoerceViaIO, ArrayCoerceExpr, ConvertRowtypeExpr,
        // CaseExpr, ArrayExpr, RowExpr, JsonExpr, etc.

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
    }
}

// Helper function for variable references
static void
ExecInitVar(Var *variable, ExprState *state, Datum *resv, bool *resnull)
{
    ExprEvalStep scratch = {0};

    // Setup appropriate opcode based on variable source
    switch (variable->varno)
    {
        case INNER_VAR:
            scratch.opcode = EEOP_INNER_VAR;
            break;
        case OUTER_VAR:
            scratch.opcode = EEOP_OUTER_VAR;
            break;
        default:
            scratch.opcode = EEOP_SCAN_VAR;
            break;
    }

    // Store variable attributes
    scratch.d.var.attnum = variable->varattno;
    scratch.d.var.vartype = variable->vartype;
    scratch.resvalue = resv;
    scratch.resnull = resnull;

    ExprEvalPushStep(state, &scratch);
}
```

*Note: This is a conceptual simplification of a 1700+ line function that handles dozens of expression node types. The actual implementation contains extensive optimizations, error handling, and specialized logic for each expression type.*