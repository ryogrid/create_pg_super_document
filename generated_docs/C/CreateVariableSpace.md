# CreateVariableSpace

## Location
[src/bin/psql/variables.c:51-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L51-L70)

## Overview
Creates and initializes a new variable space that serves as a container for managing psql variables in a linked list structure.

## Definition

```c
struct _variable *ptr;
```
## Detailed Description
This function allocates and initializes a new variable space, which is represented by a struct _variable that serves as a list header. The variable space acts as a container for storing and managing psql variables in an ordered linked list. The list entries are maintained in alphabetical name order (according to strcmp) primarily to make the output of PrintVariables() more visually pleasing and predictable.

The function creates a header node that doesn't contain actual variable data but serves as the anchor point for the linked list of variables. All fields in the header structure are initialized to NULL, establishing a clean starting state for the variable space.

## Parameters / Member Variables
This function takes no parameters and returns a newly allocated VariableSpace.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation function)
  - struct _variable (internal variable structure)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md)
  - [VariableSpace](../V/VariableSpace.md) (typedef reference)

## Notes and Other Information
- The returned structure serves as a list header, not an actual variable entry
- [Variables](../V/Variables.md) in the space are kept sorted by name for consistent output ordering
- Memory is allocated using pg_malloc, which provides error handling for allocation failures
- The header node has all fields (name, value, hooks, next) initialized to NULL
- This is part of psql's variable management system for storing user-defined and built-in variables