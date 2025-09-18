# putVariableValue

## Location
src/bin/pgbench/pgbench.c: 1852 - 1870

## Overview
Assigns a PgBenchValue (typed value) to a pgbench variable, creating the variable if it doesn't already exist.

## Definition
```c
static bool putVariableValue(Variables *variables, const char *context, char *name, const PgBenchValue *value)
```

## Detailed Description
This function sets a typed value to a pgbench variable within the specified context. Unlike putVariable which handles string values, this function works with PgBenchValue structures that can represent different data types (integers, doubles, booleans, etc.). The function handles both updating existing variables and creating new ones as needed. When a value is assigned, any existing string representation is cleared to maintain consistency.

## Parameters
- `variables`: Pointer to the Variables structure containing all pgbench variables
- `context`: String specifying the context/scope where the variable should be created or found
- `name`: Name of the variable to set (will be created if it doesn't exist)
- `value`: Pointer to PgBenchValue structure containing the typed value to assign

## Dependencies
- Functions called/Symbols referenced:
  - [lookupCreateVariable](../l/lookupCreateVariable.md)
  - free
  - [Variables](../V/Variables.md) (type)
  - PgBenchValue (type)
  - [Variable](../V/Variable.md) (type)
- Called from:
  - [putVariableInt](putVariableInt.md) (src/bin/pgbench/pgbench.c:1877)
  - [executeMetaCommand](../e/executeMetaCommand.md) (src/bin/pgbench/pgbench.c:4357)
  - [main](../m/main.md) (src/bin/pgbench/pgbench.c:7219)

## Notes and Other Information
- Returns false if the variable name is invalid or creation fails
- Clears any existing string representation (svalue) when assigning a new typed value
- Performs a structure copy of the PgBenchValue to store the value
- Part of pgbench's variable management system for storing typed data values
- Used by higher-level functions like putVariableInt for specific data type assignments