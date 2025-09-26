# _variable

## Location
[src/bin/psql/variables.h:62-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.h#L62-L71)

## Overview
The  struct represents a single variable in PostgreSQL's psql command-line client, implementing a key-value store with hook functions for validation and value transformation.

## Definition

```c
struct _variable
{
	char	   *name;
	char	   *value;
	VariableSubstituteHook substitute_hook;
	VariableAssignHook assign_hook;
	struct _variable *next;
};
```
## Detailed Description
The  struct is the fundamental building block of psql's variable management system. Each instance represents one named variable that can store a string value, along with optional hook functions that control how values are assigned and processed. The struct forms a linked list through the  pointer, allowing multiple variables to be chained together in a .

Key behavioral characteristics:
- When , the variable is logically unset but the struct is retained to preserve hook functions
- The substitute hook is called first during assignment to normalize or transform values
- The assign hook is called after substitution to validate the final value
- Variables form a singly-linked list for efficient traversal and management

## Parameters / Member Variables
- : String containing the variable name (e.g., "AUTOCOMMIT", "PROMPT1")
- : String containing the current variable value, or NULL if unset
- : Function pointer for value normalization/transformation before assignment
- : Function pointer for value validation during assignment
- : Pointer to the next variable in the linked list, forming a chain

## Dependencies
- Functions called/Symbols referenced:
  - VariableSubstituteHook (typedef)
  - VariableAssignHook (typedef)
- Called from (representative examples):
  - [CreateVariableSpace](../C/CreateVariableSpace.md)
  - [GetVariable](../G/GetVariable.md)
  - [SetVariable](../S/SetVariable.md)
  - [SetVariableHooks](../S/SetVariableHooks.md)
  - [PrintVariables](../P/PrintVariables.md)
  - [VariableHasHook](../V/VariableHasHook.md)

## Notes and Other Information
- This struct is part of psql's variable repository system, which provides an associative array-like interface for storing configuration and state
- The hook mechanism allows for sophisticated variable behavior - for example, boolean variables can use substitute hooks to normalize "off"/"on" values
- Memory management: the struct and its string members are typically managed through PostgreSQL's memory context system
- The linked list design allows for efficient iteration over all variables in a space
- Located in src/bin/psql/variables.h:62-71