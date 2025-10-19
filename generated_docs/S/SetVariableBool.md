# SetVariableBool

## Location
[src/bin/psql/variables.c:392-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L392-L403)

## Overview
Convenience function to set a variable's value to "on", effectively setting a boolean variable to true.

## Definition
```c
bool SetVariableBool(VariableSpace space, const char *name)
```

## Detailed Description
The SetVariableBool function is a simple wrapper around SetVariable that provides a convenient way to set boolean variables to true. In psql's variable system, boolean values are represented as strings, with "on" conventionally indicating true.

This function is particularly useful during psql initialization and command-line option processing where many boolean flags need to be set based on user preferences or default configurations.

The function inherits all the behavior of SetVariable, including:
- Creating the variable if it doesn't exist
- Updating the variable if it already exists
- Calling any attached substitute and assign hooks
- Proper error handling and memory management

## Parameters / Member Variables
- `space`: VariableSpace (linked list head) to operate on
- `name`: Name of the boolean variable to set to "on"

## Dependencies
- Functions called/Symbols referenced:
  - [SetVariable](SetVariable.md) (core variable setting function)
- Data types referenced:
  - [VariableSpace](../V/VariableSpace.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/psql/startup.c - setting default boolean variables)
  - [parse_psql_options](../p/parse_psql_options.md) (in src/bin/psql/startup.c - processing command-line boolean flags)

## Notes and Other Information
- Simple wrapper that always sets the value to "on" (boolean true)
- Returns the same boolean result as SetVariable (true on success, false on failure)
- Part of psql's variable system for handling boolean configuration options
- Used extensively during psql startup for setting default boolean variables
- Counterpart would be setting a variable to "off" or deleting it for boolean false

## Simplified Source

```c
bool SetVariableBool(VariableSpace space, const char *name)
{
    // Simply calls SetVariable with "on" value to represent boolean true
    return SetVariable(space, name, "on");
}
```