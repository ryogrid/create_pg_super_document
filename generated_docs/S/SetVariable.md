# SetVariable

## Location
[src/bin/psql/variables.c:211-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L211-L313)

## Overview
Sets the value of a named variable in a VariableSpace, creates a new variable if it doesn't exist, or deletes it if the value is NULL.

## Definition
```c
bool SetVariable(VariableSpace space, const char *name, const char *value)
```

## Detailed Description
The SetVariable function is the core variable management function in psql that handles setting, updating, and deleting variables in a VariableSpace. It maintains variables in a sorted linked list for efficient lookup and supports hook functions for custom processing.

Key behaviors:
- If the variable exists, it updates the value after calling substitute and assign hooks
- If the variable doesn't exist and a value is provided, it creates a new variable entry
- If value is NULL, it deletes the variable (unless it has hooks that need to be preserved)
- Variables are kept in alphabetical order by name for efficient searching
- Hook functions allow custom validation and transformation of variable values
- Memory management is handled automatically, including cleanup when variables are deleted

The function validates variable names and handles edge cases like NULL parameters gracefully.

## Parameters / Member Variables
- `space`: VariableSpace (linked list head) to operate on
- `name`: Name of the variable to set/update/delete (must be valid variable name)
- `value`: New value for the variable, or NULL to delete the variable

## Dependencies
- Functions called/Symbols referenced:
  - [valid_variable_name](../v/valid_variable_name.md) (validates variable name format)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication function)
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation function) 
  - [pg_free](../p/pg_free.md) (PostgreSQL memory deallocation function)
  - pg_log_error (PostgreSQL error logging function)
  - strcmp (standard C string comparison)
- Data types referenced:
  - [VariableSpace](../V/VariableSpace.md)
  - struct _variable
- Called from (representative examples):
  - [exec_command_set](../e/exec_command_set.md) (psql \set command)
  - [exec_command_unset](../e/exec_command_unset.md) (psql \unset command)
  - [SetVariableBool](SetVariableBool.md) (boolean variable setter)
  - [SyncVariables](SyncVariables.md)/UnsyncVariables (variable synchronization)
  - [SetResultVariables](SetResultVariables.md) (query result variables)

## Notes and Other Information
- Returns true on success, false on failure (with error message printed)
- Supports substitute hooks for value transformation before assignment
- Supports assign hooks for value validation - [assignment](../a/assignment.md) fails if hook returns false
- [Variables](../V/Variables.md) without values or hooks are automatically cleaned up to save memory
- [Variables](../V/Variables.md) are maintained in alphabetical order for efficient searching
- Part of psql's variable system, widely used throughout the codebase
- Handles memory management automatically, including proper cleanup on failures