# on_error_rollback_hook

## Location
src/bin/psql/startup.c: 1019 - 1039

## Overview
A validation hook function for the ON_ERROR_ROLLBACK psql variable that controls automatic transaction rollback behavior when SQL errors occur, with support for boolean and "interactive" modes.

## Definition


## Detailed Description
The  function is a psql variable hook that validates and processes new values assigned to the ON_ERROR_ROLLBACK variable. This variable controls whether psql automatically issues ROLLBACK TO SAVEPOINT commands when SQL errors occur within transactions, helping to recover from errors without losing the entire transaction.

The function accepts three types of values:
- "interactive": Enable automatic rollback only in interactive mode (PSQL_ERROR_ROLLBACK_INTERACTIVE) - rollback occurs when psql is reading from a terminal but not when executing scripts
- Boolean values ("on", "true", "yes", "1"): Always enable automatic rollback (PSQL_ERROR_ROLLBACK_ON)
- Boolean values ("off", "false", "no", "0"): Disable automatic rollback (PSQL_ERROR_ROLLBACK_OFF)

The "interactive" mode is particularly useful as it provides error recovery during manual database exploration while preserving script behavior where errors might be expected and handled explicitly.

## Parameters / Member Variables
- : The string value being assigned to the ON_ERROR_ROLLBACK variable that needs to be validated and converted to an enum value

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - [ParseVariableBool](../P/ParseVariableBool.md)
  - PsqlVarEnumError
  - PSQL_ERROR_ROLLBACK_INTERACTIVE
  - PSQL_ERROR_ROLLBACK_ON
  - PSQL_ERROR_ROLLBACK_OFF
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (registers the hook)

## Notes and Other Information
- This is a static function within src/bin/psql/startup.c, used internally by psql's variable system
- The function includes an Assert to ensure newval is not NULL, relying on the bool_substitute_hook to provide a default value
- Returns true if the value is successfully parsed and applied, false for invalid values
- The hook is registered in EstablishVariableSpace() alongside bool_substitute_hook for the ON_ERROR_ROLLBACK variable
- The automatic rollback feature requires that psql establish savepoints before executing commands when this mode is enabled
- The "interactive" mode represents a sophisticated balance between user convenience and predictable script behavior