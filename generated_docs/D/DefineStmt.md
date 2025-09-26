# DefineStmt

## Location
src/include/nodes/parsenodes.h: 3140 - 3150

## Overview
DefineStmt represents CREATE statements for user-defined database objects like aggregates, operators, and types in PostgreSQL parse tree.

## Definition
```c
typedef struct DefineStmt
{
    NodeTag     type;
    ObjectType  kind;           /* aggregate, operator, type */
    bool        oldstyle;       /* hack to signal old CREATE AGG syntax */
    List       *defnames;       /* qualified name (list of String) */
    List       *args;           /* a list of TypeName (if needed) */
    List       *definition;     /* a list of DefElem */
    bool        if_not_exists;  /* just do nothing if it already exists? */
    bool        replace;        /* replace if already exists? */
} DefineStmt;
```

## Detailed Description
DefineStmt is a parse tree node structure that represents CREATE statements for user-defined database objects including aggregates, operators, and types. It encapsulates all the information needed to define these objects, including their names, parameters, and definition elements. The structure supports both traditional and modern syntax variations, conditional creation, and replacement of existing objects.

## Parameters / Member Variables
- `type`: NodeTag identifier for this parse node type
- `kind`: ObjectType enumeration indicating the type of object being created (aggregate, operator, or type)
- `oldstyle`: Boolean flag to handle legacy CREATE AGGREGATE syntax compatibility
- `defnames`: List of String nodes representing the qualified name of the object being defined
- `args`: List of TypeName nodes specifying argument types (used for operators and functions)
- `definition`: List of DefElem nodes containing the object definition parameters and properties
- `if_not_exists`: Boolean flag for conditional creation - if true, no error is raised if object already exists
- `replace`: Boolean flag to indicate whether to replace an existing object with the same name

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration for object types)
- Called from (representative examples):
  - ProcessUtilitySlow
  - CreateCommandTag

## Notes and Other Information
- Part of PostgreSQL parse tree node hierarchy, inheriting from Node via NodeTag
- Used for CREATE AGGREGATE, CREATE OPERATOR, and CREATE TYPE statements
- The oldstyle flag provides backward compatibility for legacy aggregate creation syntax
- Definition list contains DefElem structures that specify object properties like state functions, operators, storage parameters, etc.
- Supports both IF NOT EXISTS and OR REPLACE semantics through separate boolean flags
- Located in src/include/nodes/parsenodes.h:3136-3150