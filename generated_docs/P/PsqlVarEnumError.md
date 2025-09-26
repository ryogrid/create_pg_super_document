# PsqlVarEnumError

## Location
src/bin/psql/variables.c: 416 - 421

## Overview
Emits standardized error messages with suggestions for variables or commands that accept enum-style arguments, providing consistent error reporting across psql.

## Definition

```c
void
PsqlVarEnumError(const char *name, const char *value, const char *suggestions)
```
## Detailed Description
The `PsqlVarEnumError` function serves as a centralized error reporting mechanism for invalid enum-style variable values in psql. It standardizes the wording and format of error messages when users provide unrecognized values for variables that accept only specific enumerated options. The function uses `pg_log_error` to output a consistently formatted message that includes the invalid value, the variable name, and a list of valid alternatives.

## Parameters / Member Variables
- `name`: The name of the variable or command that received an invalid value
- `value`: The unrecognized value that was provided by the user
- `suggestions`: A formatted string of valid options, expected to follow the format "fee, fi, fo, fum"

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_error (implicitly via macro/function call)
- Called from (representative examples):
  - echo_hook
  - echo_hidden_hook  
  - on_error_rollback_hook
  - comp_keyword_case_hook
  - histcontrol_hook
  - verbosity_hook
  - show_context_hook

## Notes and Other Information
- This function exists primarily to standardize error message wording across psql
- The suggestions parameter should be formatted as a comma-separated list
- Used extensively by variable validation hooks in psql startup and command processing
- Located in src/bin/psql/variables.c:416-421