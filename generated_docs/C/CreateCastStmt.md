# CreateCastStmt

## Location
src/include/nodes/parsenodes.h: 4002 - 4010

## Overview
CreateCastStmt represents the parsed form of a CREATE CAST SQL statement, which defines a conversion mechanism between two data types in PostgreSQL.

## Definition
```c
typedef struct CreateCastStmt
{
    NodeTag         type;
    TypeName       *sourcetype;
    TypeName       *targettype;
    ObjectWithArgs *func;
    CoercionContext context;
    bool            inout;
} CreateCastStmt;
```

## Detailed Description
CreateCastStmt is a parse node structure that holds all the information needed to create a cast in PostgreSQL. A cast defines how values of one data type can be converted to another data type. The structure captures the source type, target type, conversion function (if any), coercion context (implicit, assignment, or explicit), and whether the cast uses input/output functions.

This structure is created during the parsing phase of a CREATE CAST statement and is later processed by the command execution system to actually create the cast in the system catalogs.

## Parameters / Member Variables
- `type`: Standard NodeTag for parse tree node identification
- `sourcetype`: Pointer to TypeName representing the source data type of the cast
- `targettype`: Pointer to TypeName representing the target data type of the cast  
- `func`: Pointer to ObjectWithArgs specifying the conversion function (NULL for I/O casts)
- `context`: CoercionContext enum indicating when the cast can be applied (implicit, assignment, explicit)
- `inout`: Boolean flag indicating whether this is an I/O cast (using input/output functions)

## Dependencies
- Functions called/Symbols referenced:
  - TypeName
  - ObjectWithArgs  
  - CoercionContext
- Called from (representative examples):
  - CreateCast (command execution function)
  - ProcessUtilitySlow (utility command dispatcher)

## Notes and Other Information
- Part of the SQL DDL (Data Definition Language) parse node hierarchy
- Located in src/include/nodes/parsenodes.h along with other DDL statement structures
- The actual cast creation logic is implemented in CreateCast() in src/backend/commands/functioncmds.c
- I/O casts use the source types output function and target types input function for conversion