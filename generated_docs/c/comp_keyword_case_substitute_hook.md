# comp_keyword_case_substitute_hook

## Location
[src/bin/psql/startup.c:1040-1047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1040-L1047)

## Overview
A substitute hook function for the COMP_KEYWORD_CASE psql variable that provides a default value when the variable is unset or NULL.

## Definition

```c
static char *
comp_keyword_case_substitute_hook(char *newval)
```
## Detailed Description
This function serves as a substitute hook for the COMP_KEYWORD_CASE psql variable. When the variable is set to NULL or unset, this hook provides a sensible default value of "preserve-upper". The function is registered with psql's variable system during startup to ensure consistent behavior for keyword case completion. This hook ensures that even when no explicit value is provided for COMP_KEYWORD_CASE, the tab completion system has a valid configuration to work with.

## Parameters / Member Variables
- `newval`: The new value being set for the COMP_KEYWORD_CASE variable. If NULL, the function will substitute it with the default value.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for duplicating the default string)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (via SetVariableHooks for COMP_KEYWORD_CASE variable)

## Notes and Other Information
- The default value "preserve-upper" indicates that keyword completion should preserve uppercase letters as entered by the user
- This function is part of psql's variable hook system which allows for validation and default value substitution
- The function is static to startup.c, indicating it's only used within the psql startup module
- Located in src/bin/psql/startup.c:1040-1047