# echo_hook

## Location
[src/bin/psql/startup.c:978-997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L978-L997)

## Overview
A validation hook function for the ECHO psql variable that parses string values and sets the appropriate echo behavior mode for SQL statement display.

## Definition

```c
static bool
echo_hook(const char *newval)
```
## Detailed Description
The  function is a psql variable hook that validates and processes new values assigned to the ECHO variable. It accepts string values ("none", "errors", "queries", "all") and converts them to the corresponding internal enum values that control which SQL statements psql echoes to standard output.

The function performs case-insensitive comparison of the input string against valid options:
- "none": No echoing (PSQL_ECHO_NONE)
- "errors": Echo only failed queries (PSQL_ECHO_ERRORS)  
- "queries": Echo user-entered queries (PSQL_ECHO_QUERIES)
- "all": Echo all SQL statements including internal ones (PSQL_ECHO_ALL)

If an invalid value is provided, the function calls PsqlVarEnumError to display an appropriate error message and returns false to indicate validation failure.

## Parameters / Member Variables
- : The string value being assigned to the ECHO variable that needs to be validated and converted to an enum value

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - [PsqlVarEnumError](../P/PsqlVarEnumError.md)
  - PSQL_ECHO_QUERIES
  - PSQL_ECHO_ERRORS
  - PSQL_ECHO_ALL
  - PSQL_ECHO_NONE
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (registers the hook)

## Notes and Other Information
- This is a static function within src/bin/psql/startup.c, used internally by psql's variable system
- The function includes an Assert to ensure newval is not NULL, relying on the substitute hook to provide a default value
- Returns true if the value is successfully parsed and applied, false for invalid values
- The hook is registered in EstablishVariableSpace() alongside echo_substitute_hook for the ECHO variable
- Part of psql's variable hook system that provides type-safe enum value assignment with user-friendly string interface
- Works in conjunction with echo_substitute_hook to ensure the variable always has a valid value