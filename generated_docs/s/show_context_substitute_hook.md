# show_context_substitute_hook

## Location
[src/bin/psql/startup.c:1156-1163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1156-L1163)

## Overview
A substitute hook function for the show_context parameter in psql that provides a default value when none is specified.

## Definition

```c
static char *
show_context_substitute_hook(char *newval)
```
## Detailed Description
This function serves as a substitute hook for the show_context parameter in psql. When the show_context parameter value is NULL (not explicitly set), this function provides the default value of "errors". The function implements a simple validation and default value assignment mechanism for the show_context configuration variable.

## Parameters / Member Variables
- : The proposed new value for the show_context parameter. If NULL, the function will substitute it with the default value.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (at src/bin/psql/startup.c:1260)

## Notes and Other Information
- This is a static function defined in src/bin/psql/startup.c
- The default value "errors" is the standard setting for show_context in psql
- The function uses pg_strdup to create a copy of the default string to ensure proper memory management
- This hook is part of psql's variable system that manages configuration parameters