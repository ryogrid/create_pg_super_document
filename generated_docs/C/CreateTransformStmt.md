# CreateTransformStmt

## Location
src/include/nodes/parsenodes.h: 4016 - 4024

## Overview
CreateTransformStmt represents the parsed form of a CREATE TRANSFORM SQL statement, which defines bidirectional conversion functions between a PostgreSQL data type and a procedural language.

## Definition
```c
typedef struct CreateTransformStmt
{
    NodeTag         type;
    bool            replace;
    TypeName       *type_name;
    char           *lang;
    ObjectWithArgs *fromsql;
    ObjectWithArgs *tosql;
} CreateTransformStmt;
```

## Detailed Description
CreateTransformStmt is a parse node structure that holds information needed to create a transform in PostgreSQL. A transform defines how values of a PostgreSQL data type should be converted when passed to and from functions written in a procedural language (like PL/Python, PL/Perl, etc.). The structure captures the target data type, the procedural language name, and the conversion functions in both directions.

This structure is created during the parsing phase of a CREATE TRANSFORM statement and is later processed by the command execution system to register the transform in the system catalogs.

## Parameters / Member Variables
- `type`: Standard NodeTag for parse tree node identification
- `replace`: Boolean flag indicating whether to replace an existing transform (CREATE OR REPLACE)
- `type_name`: Pointer to TypeName representing the PostgreSQL data type for the transform
- `lang`: String name of the procedural language (e.g., "plpython3u", "plperl")
- `fromsql`: Pointer to ObjectWithArgs specifying the function to convert FROM SQL type to language type
- `tosql`: Pointer to ObjectWithArgs specifying the function to convert TO SQL type from language type

## Dependencies
- Functions called/Symbols referenced:
  - TypeName
  - ObjectWithArgs
- Called from (representative examples):
  - CreateTransform (command execution function)
  - ProcessUtilitySlow (utility command dispatcher)

## Notes and Other Information
- Part of the SQL DDL (Data Definition Language) parse node hierarchy
- Located in src/include/nodes/parsenodes.h along with other DDL statement structures
- The actual transform creation logic is implemented in CreateTransform() in src/backend/commands/functioncmds.c
- Both fromsql and tosql functions are optional - transforms can be unidirectional
- Transforms enable seamless data exchange between PostgreSQL and procedural languages