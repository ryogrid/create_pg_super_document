# VariableSpace

## Location
[src/bin/psql/variables.h:72-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.h#L72-L97)

## Overview
VariableSpace is a typedef representing a collection of variables in PostgreSQL's psql client, implemented as a pointer to a linked list of  structures that serves as a simple associative array for storing configuration settings and runtime state.

## Definition

```c
typedef struct _variable *VariableSpace;
```
## Detailed Description
VariableSpace provides an abstract data type for managing a collection of named variables in psql. It's implemented as a typedef pointing to the head of a singly-linked list of  structures. The design uses a sentinel node pattern where the first node in the list serves as a header (with NULL name/value) and actual variables are stored in subsequent nodes.

Key implementation details:
- The space is created with a sentinel head node that has NULL values for all fields
- Variables are stored in nodes following the sentinel, linked via the  pointer  
- Operations like get, set, and delete traverse the linked list to find the target variable
- The system supports both simple string storage and complex behavior via hook functions
- Memory management follows PostgreSQL's allocation patterns using pg_malloc

This design provides a simple but flexible variable repository that can be extended with validation and transformation logic through the hook mechanism.

## Parameters / Member Variables
VariableSpace is a typedef, so it doesn't have member variables directly. However, it represents a collection containing:
- Head node: Sentinel  struct with NULL name/value serving as list anchor
- Variable nodes: Linked  structures containing actual name-value pairs and hooks

## Dependencies
- Functions called/Symbols referenced:
  - [_variable](../v/_variable.md) (underlying struct type)
  - VariableSubstituteHook (typedef for value transformation)
  - VariableAssignHook (typedef for value validation)
- Called from (representative examples):
  - [CreateVariableSpace](../C/CreateVariableSpace.md) (creates new space)
  - [GetVariable](../G/GetVariable.md) (retrieves variable values)
  - [SetVariable](../S/SetVariable.md) (assigns variable values)
  - [PrintVariables](../P/PrintVariables.md) (displays all variables)
  - [SetVariableHooks](../S/SetVariableHooks.md) (configures hook functions)
  - [DeleteVariable](../D/DeleteVariable.md) (removes variables)
  - [_psqlSettings](../p/_psqlSettings.md) (main psql settings structure)

## Notes and Other Information
- Used in psql's main settings structure (_psqlSettings.vars) to store shell variables like AUTOCOMMIT, PROMPT1, etc.
- The sentinel node design simplifies list manipulation operations by eliminating special cases for empty lists
- [Variables](Variables.md) can be logically unset (value == NULL) while preserving their hook functions
- The linked list implementation provides O(n) access time but is sufficient for the typically small number of psql variables
- Memory allocated for VariableSpace and its variables should be cleaned up when the psql session ends
- Located in src/bin/psql/variables.h:72 as a typedef definition