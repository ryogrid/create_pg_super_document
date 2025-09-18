# eval_windowfunction

## Location
src/backend/executor/nodeWindowAgg.c: 1033 - 1080

## Overview
This is a static function that executes a window function call, handling the special context and memory management required for window function evaluation.

## Definition
```c
static void eval_windowfunction(WindowAggState *winstate, WindowStatePerFunc perfuncstate, Datum *result, bool *isnull)
```

## Detailed Description
The `eval_windowfunction` function is responsible for invoking window functions during query execution. Unlike regular functions, window functions require special handling because they need random access to arbitrary rows within their partition. The function sets up the proper execution context, initializes function call information with the window object context, and handles memory management to ensure results are properly copied when multiple window functions are being evaluated.

The function operates in the per-tuple memory context and uses the WindowObject (passed through fcinfo->context) to provide window functions access to partition data through special functions like WinGetFuncArgInPartition and WinGetFuncArgInFrame.

## Parameters / Member Variables
- `winstate`: The WindowAggState containing the overall state of window aggregation execution
- `perfuncstate`: The WindowStatePerFunc containing per-function state information including function info and window object
- `result`: Output parameter to store the computed result datum
- `isnull`: Output parameter to indicate if the result is null

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO
  - FUNC_MAX_ARGS  
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [ExecWindowAgg](../E/ExecWindowAgg.md)

## Notes and Other Information
- Window function arguments are not evaluated in this function; instead, window functions use special accessor functions to retrieve argument values from specific rows
- The function handles memory management by copying pass-by-ref results when multiple window functions are using the same WindowObject to prevent data clobbering
- All regular argument slots are set to null since window functions access their arguments through the WindowObject context
- The function temporarily switches to per-tuple memory context during execution