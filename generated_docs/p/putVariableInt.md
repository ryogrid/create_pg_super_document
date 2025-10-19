# putVariableInt

## Location
[src/bin/pgbench/pgbench.c:1871-1888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1871-L1888)

## Overview
Assigns an integer value to a pgbench variable, creating the variable if it doesn't already exist.

## Definition
```c
static bool putVariableInt(Variables *variables, const char *context, char *name, int64 value)
```

## Detailed Description
This function provides a convenient wrapper for assigning integer values to pgbench variables. It creates a PgBenchValue structure with the specified integer value and delegates the actual assignment to putVariableValue. This abstraction simplifies the process of storing integer values in the variable system without requiring callers to manually construct PgBenchValue structures.

## Parameters
- `variables`: Pointer to the Variables structure containing all pgbench variables
- `context`: String specifying the context/scope where the variable should be created or found
- `name`: Name of the variable to set (will be created if it doesn't exist)
- `value`: 64-bit integer value to assign to the variable

## Dependencies
- Functions called/Symbols referenced:
  - [setIntValue](../s/setIntValue.md)
  - [putVariableValue](putVariableValue.md)
  - [Variables](../V/Variables.md) (type)
  - PgBenchValue (type)
- Called from:
  - [runShellCommand](../r/runShellCommand.md) (src/bin/pgbench/pgbench.c:3016)
  - [main](../m/main.md) (src/bin/pgbench/pgbench.c:7264, 7276, 7286, 7295)

## Notes and Other Information
- Returns false if the variable name is invalid or creation fails
- Uses setIntValue to properly initialize the PgBenchValue structure with integer type
- Part of pgbench's variable management system, providing type-specific convenience functions
- Commonly used in the main function to set various configuration and status variables
- Delegates to putVariableValue for the actual storage operation

## Simplified Source

```c
static bool
putVariableInt(Variables *variables, const char *context, char *name,
               int64 value)
{
    PgBenchValue val;

    // Create integer value and delegate to putVariableValue
    setIntValue(&val, value);
    return putVariableValue(variables, context, name, &val);
}
```