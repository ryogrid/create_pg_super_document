# histcontrol_substitute_hook

## Location
src/bin/psql/startup.c: 1069 - 1076

## Overview
A substitute hook function for the HISTCONTROL psql variable that provides a default value when the variable is unset or NULL.

## Definition
static char *histcontrol_substitute_hook(char *newval)

## Detailed Description
This function serves as a substitute hook for the HISTCONTROL psql variable. When the variable is set to NULL or unset, this hook provides a sensible default value of "none". The function is registered with psql's variable system during startup to ensure consistent behavior for history control settings. This hook ensures that even when no explicit value is provided for HISTCONTROL, the command history system has a valid configuration to work with.

## Parameters / Member Variables
- `newval`: The new value being set for the HISTCONTROL variable. If NULL, the function will substitute it with the default value "none".

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for duplicating the default string)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (via SetVariableHooks for HISTCONTROL variable)

## Notes and Other Information
- The default value "none" indicates that no special history control behavior is enabled by default
- This function is part of psql's variable hook system which allows for validation and default value substitution
- The function is static to startup.c, indicating it's only used within the psql startup module
- Works in conjunction with histcontrol_hook which validates and processes the actual HISTCONTROL values
- Located in src/bin/psql/startup.c:1069-1076