# echo_hidden_hook

## Location
src/bin/psql/startup.c: 998 - 1018

## Overview
A validation hook function for the ECHO_HIDDEN psql variable that controls the display of internally generated SQL commands with support for boolean and special "noexec" values.

## Definition


## Detailed Description
The  function is a psql variable hook that validates and processes new values assigned to the ECHO_HIDDEN variable. This variable controls whether psql displays the SQL commands that are generated internally by backslash commands (like \d, \dt, etc.).

The function accepts three types of values:
- "noexec": Show hidden commands but don't execute them (PSQL_ECHO_HIDDEN_NOEXEC) - useful for debugging
- Boolean values ("on", "true", "yes", "1"): Enable echoing of hidden commands (PSQL_ECHO_HIDDEN_ON)
- Boolean values ("off", "false", "no", "0"): Disable echoing of hidden commands (PSQL_ECHO_HIDDEN_OFF)

The "noexec" option is particularly useful for developers and advanced users who want to see what SQL commands psql generates internally without actually executing them, which helps in understanding how psql implements various features.

## Parameters / Member Variables
- : The string value being assigned to the ECHO_HIDDEN variable that needs to be validated and converted to an enum value

## Dependencies
- Functions called/Symbols referenced:
  - pg_strcasecmp
  - ParseVariableBool
  - PsqlVarEnumError
  - PSQL_ECHO_HIDDEN_NOEXEC
  - PSQL_ECHO_HIDDEN_ON
  - PSQL_ECHO_HIDDEN_OFF
- Called from (representative examples):
  - EstablishVariableSpace (registers the hook)

## Notes and Other Information
- This is a static function within src/bin/psql/startup.c, used internally by psql's variable system
- The function includes an Assert to ensure newval is not NULL, relying on the bool_substitute_hook to provide a default value
- Returns true if the value is successfully parsed and applied, false for invalid values
- The hook is registered in EstablishVariableSpace() alongside bool_substitute_hook for the ECHO_HIDDEN variable
- Supports both boolean parsing and a special "noexec" mode that is unique among psql variables
- The "noexec" feature is valuable for educational purposes and debugging psql's internal command generation