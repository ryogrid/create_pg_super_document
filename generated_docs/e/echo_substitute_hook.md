# echo_substitute_hook

## Location
[src/bin/psql/startup.c:970-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L970-L977)

## Overview
A substitute hook function for the ECHO psql variable that provides a default value when the variable is unset or null.

## Definition

```c
static char *
echo_substitute_hook(char *newval)
```
## Detailed Description
The  function is a psql variable substitute hook that ensures the ECHO variable always has a valid string value. When the ECHO variable is set to NULL (unset), this hook automatically substitutes it with the default value "none". This prevents the variable from being in an undefined state and ensures consistent behavior.

The ECHO variable controls which SQL statements psql echoes to standard output. Valid values include "none" (no echoing), "queries" (echo user queries), "errors" (echo failed queries), and "all" (echo all statements). The substitute hook ensures that when the variable is unset, it defaults to "none" behavior.

This hook is part of psql's variable system that provides preprocessing and default value handling for configuration variables.

## Parameters / Member Variables
- `*newval`: The string value being assigned to the ECHO variable; may be NULL if the variable is being unset
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (implicitly called to duplicate the "none" string)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (registers the hook)

## Notes and Other Information
- This is a static function within src/bin/psql/startup.c, used internally by psql's variable system
- Returns the original value if not NULL, or a newly allocated "none" string if the input is NULL
- The hook is registered in EstablishVariableSpace() alongside the echo_hook validation hook for the ECHO variable
- Part of psql's variable hook system that ensures variables always have valid, meaningful values
- The substitute hook runs before the validation hook in the variable processing pipeline

## Simplified Source

```c
static char *
echo_substitute_hook(char *newval)
{
    // Default to "none" when ECHO variable is unset
    if (newval == NULL)
        newval = pg_strdup("none");

    return newval;
}
```