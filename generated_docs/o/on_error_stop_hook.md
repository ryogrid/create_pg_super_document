# on_error_stop_hook

## Location
[src/bin/psql/startup.c:881-886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L881-L886)

## Overview
A hook function used in PostgreSQL's psql client to validate and set the ON_ERROR_STOP variable, which controls whether psql should stop processing commands when an error occurs.

## Definition

```c
static bool
on_error_stop_hook(const char *newval)
```
## Detailed Description
The  function serves as a validation and assignment hook for the ON_ERROR_STOP psql variable. It is called whenever the user attempts to set the ON_ERROR_STOP variable through psql commands like . The function uses the  utility to parse the string value and convert it to a boolean, storing the result in the global  field. This hook ensures that only valid boolean values (like "on", "off", "true", "false", etc.) are accepted for the ON_ERROR_STOP setting.

## Parameters / Member Variables
- `*newval`: A string containing the new value to be assigned to the ON_ERROR_STOP variable
## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableBool](../P/ParseVariableBool.md)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)

## Notes and Other Information
- This is a static function within the psql startup module
- The ON_ERROR_STOP variable controls psql's behavior when SQL commands result in errors
- When ON_ERROR_STOP is enabled, psql will terminate script execution upon encountering an error
- The function returns true if the value was successfully parsed and set, false otherwise
- Located in src/bin/psql/startup.c at lines 881-886

## Simplified Source

```c
static bool on_error_stop_hook(const char *newval) {
    // Parse and validate boolean value, then set error stop flag
    return ParseVariableBool(newval, "ON_ERROR_STOP", &pset.on_error_stop);
}
```

This hook function:
1. Validates the new string value as a boolean
2. Sets the global ON_ERROR_STOP flag if valid
3. Controls whether psql stops on SQL errors during script execution
4. Returns true on success, false on invalid input