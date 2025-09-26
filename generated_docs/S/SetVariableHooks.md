# SetVariableHooks

## Location
src/bin/psql/variables.c: 314 - 366

## Overview
Attaches substitute and/or assign hook functions to a named variable, creating the variable if it doesn't exist.

## Definition
```c
void SetVariableHooks(VariableSpace space, const char *name,
                      VariableSubstituteHook shook,
                      VariableAssignHook ahook)
```

## Detailed Description
The SetVariableHooks function is used to attach callback functions to variables in psql's variable system. It allows for custom processing whenever a variable's value is accessed or modified through substitute and assign hooks.

Key behaviors:
- If the variable exists, updates its hook functions and immediately executes them
- If the variable doesn't exist, creates it with a NULL value and attached hooks
- The substitute hook is called immediately after being set to potentially transform the current value
- The assign hook is called immediately after being set to validate/process the current value
- Either hook can be NULL if only one type of hook is needed
- Maintains the alphabetical ordering of variables in the linked list

The function is typically used during psql initialization to establish special variables that need custom behavior, such as variables that synchronize with internal psql state.

## Parameters / Member Variables
- `space`: VariableSpace (linked list head) to operate on
- `name`: Name of the variable to attach hooks to (must be valid variable name)
- `shook`: VariableSubstituteHook function pointer, or NULL if not needed
- `ahook`: VariableAssignHook function pointer, or NULL if not needed

## Dependencies
- Functions called/Symbols referenced:
  - valid_variable_name (validates variable name format)
  - pg_strdup (PostgreSQL string duplication function)
  - pg_malloc (PostgreSQL memory allocation function)
  - strcmp (standard C string comparison)
- Data types referenced:
  - VariableSpace
  - struct _variable
  - VariableSubstituteHook (function pointer type)
  - VariableAssignHook (function pointer type)
- Called from (representative examples):
  - EstablishVariableSpace (during psql initialization for setting up built-in variable hooks)

## Notes and Other Information
- Creates variables with NULL values if they don't exist, just to hold the hooks
- Immediately executes both hooks after setting them to initialize derived state
- Hook execution failures are ignored during setup - this is expected since no user value has been assigned yet
- Part of psql's variable system initialization - used extensively in EstablishVariableSpace
- Enables sophisticated variable behaviors like auto-sync with psql internal state
- No return value - function always succeeds or silently fails for invalid inputs