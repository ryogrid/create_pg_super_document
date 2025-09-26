# DeleteVariable

## Location
[src/bin/psql/variables.c:404-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L404-L415)

## Overview
Attempts to delete a variable from the specified variable space, with deletion of nonexistent variables considered a non-error operation.

## Definition

```c
bool
DeleteVariable(VariableSpace space, const char *name)
```
## Detailed Description
The  function provides a convenient wrapper for deleting variables from psql's variable storage system. It internally delegates to  with a NULL value, which effectively removes the variable from the space. The function implements a forgiving deletion policy where attempting to delete a nonexistent variable does not result in an error condition, making it safe to use in cleanup operations where the existence of the variable is uncertain.

## Parameters / Member Variables
- : The VariableSpace from which to delete the variable
- : The name of the variable to delete (const char pointer)

## Dependencies
- Functions called/Symbols referenced:
  - [SetVariable](../S/SetVariable.md)
  - [VariableSpace](../V/VariableSpace.md) (type)
- Called from (representative examples):
  - [parse_psql_options](../p/parse_psql_options.md)

## Notes and Other Information
- Deleting a nonexistent variable is explicitly documented as not being an error
- The function returns the result of the underlying  call
- Located in src/bin/psql/variables.c:404-415