# quiet_hook

## Location
[src/bin/psql/startup.c:887-892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L887-L892)

## Overview
A hook function used in PostgreSQL's psql client to validate and set the QUIET variable, which controls whether psql should suppress informational output and run in quiet mode.

## Definition

```c
static bool
quiet_hook(const char *newval)
```
## Detailed Description
The  function serves as a validation and assignment hook for the QUIET psql variable. It is called whenever the user attempts to set the QUIET variable through psql commands like . The function uses the  utility to parse the string value and convert it to a boolean, storing the result in the global  field. This hook ensures that only valid boolean values (like "on", "off", "true", "false", etc.) are accepted for the QUIET setting. When QUIET mode is enabled, psql suppresses various informational messages and runs more silently.

## Parameters / Member Variables
- `*newval`: A string containing the new value to be assigned to the QUIET variable
## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableBool](../P/ParseVariableBool.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module
- The QUIET variable controls psql's verbosity level
- When QUIET is enabled, psql suppresses informational output like startup messages and command acknowledgments
- The function returns true if the value was successfully parsed and set, false otherwise
- Located in src/bin/psql/startup.c at lines 887-892
- Useful for automated scripts where minimal output is desired

## Simplified Source

```c
static bool quiet_hook(const char *newval) {
    // Parse and validate boolean value, then set quiet mode flag
    return ParseVariableBool(newval, "QUIET", &pset.quiet);
}
```

This hook function:
1. Validates the new string value as a boolean
2. Sets the global QUIET flag if valid
3. Controls psql's verbosity (suppresses informational messages when enabled)
4. Returns true on success, false on invalid input