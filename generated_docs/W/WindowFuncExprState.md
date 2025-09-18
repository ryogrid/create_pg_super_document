# WindowFuncExprState

## Location
src/include/nodes/execnodes.h: 871 - 878

## Overview
WindowFuncExprState represents the execution state for window function expressions, maintaining runtime information needed to evaluate window functions during query execution.

## Definition
```c
typedef struct WindowFuncExprState
{
    NodeTag     type;
    WindowFunc *wfunc;      /* expression plan node */
    List       *args;       /* ExprStates for argument expressions */
    ExprState  *aggfilter;  /* FILTER expression */
    int         wfuncno;    /* ID number for wfunc within its plan node */
} WindowFuncExprState;
```

## Detailed Description
WindowFuncExprState serves as the execution state node for window function expressions within PostgreSQLs expression evaluation framework. Unlike regular function calls, window functions require special handling due to their dependency on window frames and partitioning, necessitating dedicated state management.

This structure maintains the connection between the planned window function expression (WindowFunc) and its runtime execution state, including prepared argument expressions and filter conditions. The wfuncno field provides a unique identifier that allows the window aggregation node to associate this expression state with the appropriate window function computation context.

The structure integrates with PostgreSQLs expression compilation system, enabling both interpreted and JIT-compiled execution paths for window function evaluation. The separation of argument preparation from function execution allows for optimized evaluation patterns where arguments can be computed once and reused across multiple window frame calculations.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a WindowFuncExprState node for runtime type checking
- `wfunc`: Pointer to the corresponding WindowFunc plan node containing the function definition and configuration
- `args`: List of ExprState nodes representing the prepared argument expressions for the window function
- `aggfilter`: ExprState for the FILTER clause expression, used to conditionally include rows in window function calculation (NULL if no filter)
- `wfuncno`: Unique identifier number for this window function within its containing plan node, used for state association

## Dependencies
- Functions called/Symbols referenced:
  - WindowFunc (corresponding plan node)
  - NodeTag (node type identification)
  - [List](../L/List.md) (argument expression list)
  - ExprState (expression evaluation state)
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (src/backend/executor/execExpr.c:1075)
  - [ExecInterpExpr](../E/ExecInterpExpr.md) (src/backend/executor/execExprInterp.c:1624)
  - [advance_windowaggregate](../a/advance_windowaggregate.md) (src/backend/executor/nodeWindowAgg.c:247)
  - [advance_windowaggregate_base](../a/advance_windowaggregate_base.md) (src/backend/executor/nodeWindowAgg.c:424)
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md) (src/backend/executor/nodeWindowAgg.c:2555)
  - llvm_compile_expr (src/backend/jit/llvm/llvmjit_expr.c:2094)

## Notes and Other Information
- Part of the expression state node hierarchy, specifically designed for window function expressions that require special execution context
- The wfuncno field enables efficient mapping between expression state and window function computation state in the WindowAgg node
- FILTER clause support allows for conditional window function evaluation, commonly used with aggregate window functions
- Integrates with both interpreted and JIT-compiled expression evaluation systems
- Used extensively in WindowStatePerFuncData for maintaining per-function state during window aggregation
- The args list contains pre-compiled expression states for efficient argument evaluation during window frame processing
- Expression state separation allows for argument reuse across multiple evaluations within the same window frame
- Supports the full range of PostgreSQL window functions including ranking, value, and aggregate functions