# bind_param_error_callback

## Location
src/backend/tcop/postgres.c: 2576 - 2624

## Overview
Error context callback function that provides detailed parameter information when errors occur during bind message parameter parsing.

## Definition
```c
static void bind_param_error_callback(void *arg)
```

## Detailed Description
This function serves as an error context callback specifically used during the parsing of parameters in a Bind message. When an error occurs while processing parameter data, this callback provides additional context by displaying the parameter number, value (if available), and associated portal name. The parameter value is properly quoted and truncated according to the log_parameter_max_length_on_error configuration setting.

The function handles both named and unnamed portals, providing appropriate context messages for debugging parameter-related errors in prepared statements.

## Parameters / Member Variables
- `arg`: Void pointer that is cast to BindParamCbData structure containing:
  - `paramno`: Parameter number (0-based index)
  - `paramval`: Textual representation of parameter value (may be NULL)
  - `portalName`: Name of the portal being bound (may be NULL or empty)

## Dependencies
- Functions called/Symbols referenced:
  - [BindParamCbData](../B/BindParamCbData.md) (callback data structure)
  - [StringInfoData](../S/StringInfoData.md) (string buffer structure)
  - initStringInfo (initializes string buffer)
  - [appendStringInfoStringQuoted](../a/appendStringInfoStringQuoted.md) (adds quoted string with length limit)
  - errcontext (adds context to error messages)
  - [pfree](../p/pfree.md) (frees memory)
  - log_parameter_max_length_on_error (configuration variable)
- Called from (representative examples):
  - [exec_bind_message](../e/exec_bind_message.md) (as error callback during parameter parsing)

## Notes and Other Information
- Used exclusively as an error callback function during bind message processing
- Properly handles NULL parameter values and empty portal names
- Parameter numbers are displayed as 1-based to match SQL convention (paramno + 1)
- Parameter values are quoted and may be truncated for logging safety
- Provides different error context messages for named vs unnamed portals
- Essential for debugging parameter binding errors in the extended query protocol
- Part of PostgreSQL's comprehensive error context and reporting system