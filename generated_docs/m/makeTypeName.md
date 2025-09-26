# makeTypeName

## Location
src/backend/nodes/makefuncs.c: 493 - 504

## Overview
Creates a TypeName node for an unqualified type name, serving as a convenience function for building simple type name structures in the PostgreSQL parser.

## Definition

```c
TypeName *
makeTypeName(char *typnam)
```
## Detailed Description
The  function is a utility function that constructs a TypeName node for a single, unqualified type name. It acts as a wrapper around , simplifying the creation of TypeName nodes when dealing with basic type names that don't require schema qualification. The function sets up the TypeName with default type modifier settings that can be adjusted later by the caller.

This function is commonly used in the PostgreSQL parser when processing SQL statements that reference simple data types without explicit schema qualification.

## Parameters / Member Variables
- : A character string containing the name of the type to be represented in the TypeName node

## Dependencies
- Functions called/Symbols referenced:
  - makeTypeNameFromNameList
  - makeString
  - list_make1
- Called from (representative examples):
  - Various parser functions and SQL processing routines

## Notes and Other Information
- The type modifier (typmod) is set to default values but can be modified by the caller after creation
- This function provides a simplified interface for the common case of unqualified type names
- The resulting TypeName node contains a single-element list with the provided type name
- Declared in src/include/nodes/makefuncs.h at line 72