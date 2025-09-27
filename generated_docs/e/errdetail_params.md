# errdetail_params

## Location
[src/backend/tcop/postgres.c:2503-2522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2503-L2522)

## Overview
Adds parameter binding information to error messages when bind parameters are available, specifically used for statement logging.

## Definition
```c
static int errdetail_params(ParamListInfo params)
```

## Detailed Description
This function enhances error messages by including the values of bound parameters when they are available. It is specifically designed for statement logging purposes and respects the log_parameter_max_length configuration setting (not log_parameter_max_length_on_error). The function formats parameter values into a readable string and includes them in error details to aid in debugging prepared statements and parameterized queries.

The function only adds parameter information if parameters exist, contain data, and logging of parameters is enabled via configuration.

## Parameters / Member Variables
- `params`: ParamListInfo structure containing parameter binding information, including parameter values and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [ParamListInfo](../P/ParamListInfo.md) (parameter list structure)
  - [BuildParamLogString](../B/BuildParamLogString.md) (formats parameters for logging)
  - [errdetail](errdetail.md) (adds detail to error messages)
  - log_parameter_max_length (configuration variable)
- Called from (representative examples):
  - [exec_bind_message](exec_bind_message.md) (during bind message processing)
  - [exec_execute_message](exec_execute_message.md) (during execute message processing)

## Notes and Other Information
- Returns 0 in all cases (return value appears to be unused)
- Controlled by log_parameter_max_length configuration, not log_parameter_max_length_on_error
- Used specifically for statement logging rather than general error reporting
- Safely handles NULL or empty parameter lists
- Parameter values are truncated based on the configured maximum length
- Part of PostgreSQL's extended query protocol error reporting system

## Simplified Source

```c
// Simplified version of errdetail_params
static int errdetail_params(ParamListInfo params) {
    // Check if we have parameters and logging is enabled
    if (params && params->numParams > 0 && log_parameter_max_length != 0) {
        // Build parameter string for logging
        char *param_string = BuildParamLogString(params, NULL, log_parameter_max_length);

        // Add parameter details to error message if we have content
        if (param_string && param_string[0] != '\0') {
            errdetail("Parameters: %s", param_string);
        }
    }

    return 0;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Used more descriptive variable name (param_string instead of str)
- Preserved the essential parameter validation and logging logic
- Maintained the core functionality while improving readability