# WindowStatePerFuncData

## Location
src/backend/executor/nodeWindowAgg.c: 78 - 101

## Overview
WindowStatePerFuncData maintains per-function working state for each window function and window aggregate handled by a WindowAgg executor node.

## Definition
```c
typedef struct WindowStatePerFuncData
{
    /* Links to WindowFunc expr and state nodes this working state is for */
    WindowFuncExprState *wfuncstate;
    WindowFunc *wfunc;

    int         numArguments;   /* number of arguments */

    FmgrInfo    flinfo;         /* fmgr lookup data for window function */

    Oid         winCollation;   /* collation derived for window function */

    /*
     * We need the len and byval info for the result of each function in order
     * to know how to copy/delete values.
     */
    int16       resulttypeLen;
    bool        resulttypeByVal;

    bool        plain_agg;      /* is it just a plain aggregate function? */
    int         aggno;          /* if so, index of its WindowStatePerAggData */

    WindowObject winobj;        /* object used in window function API */
} WindowStatePerFuncData;
```

## Detailed Description
WindowStatePerFuncData is a crucial structure in PostgreSQL's window function implementation that maintains the execution state for individual window functions. Each window function or aggregate processed by a WindowAgg node has its own instance of this structure. It serves as a bridge between the high-level window function expression and the low-level execution machinery, storing function metadata, type information, and the WindowObject used for API calls.

The structure handles both regular window functions and plain aggregates used as window functions. When dealing with plain aggregates, it maintains a reference to the corresponding WindowStatePerAggData through the aggno field. The WindowObject (winobj) contained within provides the interface for the window function to access rows and manage its execution state.

## Parameters / Member Variables
- `wfuncstate`: Pointer to the WindowFuncExprState representing the expression state for this window function
- `wfunc`: Pointer to the WindowFunc node containing the window function definition
- `numArguments`: Number of arguments passed to the window function
- `flinfo`: Function manager information for efficient function calls
- `winCollation`: Collation information derived for this window function's operation
- `resulttypeLen`: Length of the result type for memory management operations
- `resulttypeByVal`: Whether the result type is passed by value or reference
- `plain_agg`: Boolean flag indicating if this is a plain aggregate function used as a window function
- `aggno`: Index into WindowStatePerAggData array if this is a plain aggregate
- `winobj`: WindowObject instance used for window function API calls

## Dependencies
- Functions called/Symbols referenced:
  - [WindowFuncExprState](WindowFuncExprState.md)
  - WindowFunc
  - WindowObject
- Called from (representative examples):
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md)
  - [WindowStatePerFunc](WindowStatePerFunc.md) (typedef)

## Notes and Other Information
- This structure is allocated once per window function during executor initialization and persists throughout query execution
- The distinction between regular window functions and plain aggregates is important for optimization - plain aggregates can often be computed more efficiently
- The WindowObject contained within is the primary interface used by window functions to access the windowing API
- Function metadata is cached in this structure to avoid repeated lookups during execution
- Type information (resulttypeLen, resulttypeByVal) is critical for proper memory management of function results