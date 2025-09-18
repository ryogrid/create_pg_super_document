# leadlag_common

## Location
[src/backend/utils/adt/windowfuncs.c:528-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L528-L579)

## Overview
This static function provides the common implementation for both LAG() and LEAD() window functions, handling offset and default value parameters with directional logic.

## Definition
```c
static Datum leadlag_common(FunctionCallInfo fcinfo, bool forward, bool withoffset, bool withdefault)
```

## Detailed Description
The leadlag_common function implements the core logic shared by the LAG() and LEAD() window functions. It handles accessing rows at a specified offset from the current row within a partition, with optional default values when the target row is out of bounds.

The function works as follows:
1. Determines the offset value (either from a parameter or defaults to 1)
2. Checks if the offset parameter is constant (for optimization purposes)
3. Uses WinGetFuncArgInPartition to fetch the value from the target row
4. If the target row is outside the partition boundary and a default value is provided, returns the default value
5. Otherwise returns NULL if the target row is out of bounds or the retrieved value is NULL

The direction (forward for LEAD, backward for LAG) is controlled by the `forward` parameter, and optional features are controlled by `withoffset` and `withdefault` flags.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo containing the function call context and arguments
- `forward`: Boolean indicating direction (true for LEAD, false for LAG)  
- `withoffset`: Boolean indicating whether an offset parameter was provided
- `withdefault`: Boolean indicating whether a default value parameter was provided

## Dependencies
- Functions called/Symbols referenced:
  - PG_WINDOW_OBJECT
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [WinGetFuncArgCurrent](../W/WinGetFuncArgCurrent.md)
  - [get_fn_expr_arg_stable](../g/get_fn_expr_arg_stable.md)
  - [WinGetFuncArgInPartition](../W/WinGetFuncArgInPartition.md)
  - WINDOW_SEEK_CURRENT
  - PG_RETURN_DATUM
- Called from (representative examples):
  - [window_lag](../w/window_lag.md)
  - [window_lag_with_offset](../w/window_lag_with_offset.md)
  - [window_lag_with_offset_and_default](../w/window_lag_with_offset_and_default.md)
  - [window_lead](../w/window_lead.md)
  - [window_lead_with_offset](../w/window_lead_with_offset.md)
  - [window_lead_with_offset_and_default](../w/window_lead_with_offset_and_default.md)

## Notes and Other Information
- This is a static helper function that consolidates the common logic between LAG and LEAD functions
- The offset can be positive or negative depending on the direction; the function applies the correct sign internally
- Uses WINDOW_SEEK_CURRENT as the seek mode for row positioning
- Optimizes performance by checking if the offset is constant using get_fn_expr_arg_stable()
- Returns NULL when the offset parameter is NULL, the target row is out of bounds with no default, or when the retrieved value is NULL
- Located in src/backend/utils/adt/windowfuncs.c:528-579