# WinGetFuncArgCurrent

## Location
[src/backend/executor/nodeWindowAgg.c:3593-3606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3593-L3606)

## Overview
Evaluates a window function's argument expression on the current row, providing a simple and efficient way to access argument values from the row being processed.

## Definition

```c
Datum
WinGetFuncArgCurrent(WindowObject winobj, int argno, bool *isnull)
```
## Detailed Description
This function provides the most straightforward way to evaluate window function arguments on the current row being processed. Unlike WinGetFuncArgInPartition and WinGetFuncArgInFrame, it doesn't need to fetch tuples from other positions or handle complex frame semantics.

The function operates by:
1. Validating the window object and extracting the window state
2. Setting up the expression context to use the current scan tuple slot
3. Directly evaluating the specified argument expression on the current row
4. Returning the result along with null status information

This function is specifically designed for "ordinary" window function arguments that should be evaluated on the current row, such as the offset parameter in LAG/LEAD functions or the bucket count in NTILE. It will succeed even when the window object's mark has been positioned beyond the current row, making it reliable for parameter evaluation.

## Parameters / Member Variables
- `winobj`: Window object containing the argument expressions and state
- `argno`: Zero-based index of the argument expression to evaluate
- `*isnull`: Output parameter receiving null status of the evaluated expression
## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [list_nth](../l/list_nth.md)
- Called from (representative examples):
  - [window_ntile](../w/window_ntile.md)
  - [leadlag_common](../l/leadlag_common.md)
  - [window_nth_value](../w/window_nth_value.md)

## Notes and Other Information
- This is the most efficient way to evaluate arguments on the current row as it doesn't require tuple fetching
- Works regardless of the window object's mark position, unlike the other WinGetFuncArg variants
- Specifically intended for function parameters rather than data values (e.g., the 'N' in LAG(col, N))
- Uses the current scan tuple slot directly, avoiding the overhead of temporary tuple management
- Essential for window functions that need to evaluate parameter expressions in the context of the current row
- Simpler and faster than using WinGetFuncArgInPartition with WINDOW_SEEK_CURRENT and relpos=0