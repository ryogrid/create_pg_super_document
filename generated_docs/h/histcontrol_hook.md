# histcontrol_hook

## Location
[src/bin/psql/startup.c:1077-1097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1077-L1097)

## Overview
A validation and assignment hook function for the HISTCONTROL psql variable that parses and validates user input to set the command history control behavior.

## Definition
static bool histcontrol_hook(const char *newval)

## Detailed Description
This function serves as a validation hook for the HISTCONTROL psql variable. It parses the provided string value and sets the corresponding history control behavior in the global pset structure. The function validates that the input is one of the four supported history control modes and returns false if an invalid value is provided. When successful, it updates pset.histcontrol with the appropriate enum value that controls how command history is managed in the psql session.

## Parameters / Member Variables
- `newval`: The new string value being assigned to the HISTCONTROL variable. Must be one of: "none", "ignorespace", "ignoredups", or "ignoreboth".

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (for case-insensitive string comparison)  
  - PsqlVarEnumError (for error reporting on invalid values)
  - hctl_none (enum constant for no history control)
  - hctl_ignorespace (enum constant for ignoring commands starting with space)
  - hctl_ignoredups (enum constant for ignoring duplicate commands)
  - hctl_ignoreboth (enum constant combining ignorespace and ignoredups)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (via SetVariableHooks for HISTCONTROL variable)

## Notes and Other Information
- The function expects newval to never be NULL due to the substitute hook providing a default value
- Supports four history control modes: none (save all), ignorespace (ignore leading space commands), ignoredups (ignore duplicates), ignoreboth (combine both behaviors)
- Uses case-insensitive comparison allowing flexibility in user input
- Returns false on validation failure, preventing the variable assignment
- Updates the global pset.histcontrol field which is used by the command history system
- The ignoreboth option combines both ignorespace and ignoredups behaviors using bitwise OR
- Located in src/bin/psql/startup.c:1077-1097