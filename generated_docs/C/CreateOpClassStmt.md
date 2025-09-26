# CreateOpClassStmt

## Location
src/include/nodes/parsenodes.h: 3169 - 3178

## Overview
CreateOpClassStmt represents a CREATE OPERATOR CLASS statement in PostgreSQL parse tree, used to define a new operator class for an access method.

## Definition
```c
typedef struct CreateOpClassStmt
{
    NodeTag     type;
    List       *opclassname;     /* qualified name (list of String) */
    List       *opfamilyname;    /* qualified name (ditto); NIL if omitted */
    char       *amname;          /* name of index AM opclass is for */
    TypeName   *datatype;        /* datatype of indexed column */
    List       *items;           /* List of CreateOpClassItem nodes */
    bool        isDefault;       /* Should be marked as default for type? */
} CreateOpClassStmt;
```

## Detailed Description
CreateOpClassStmt is a parse tree node structure that encapsulates information needed to create an operator class in PostgreSQL. Operator classes define sets of operations that can be used with specific access methods (like B-tree, GiST, GIN, etc.) for indexing particular data types. The structure contains the operator class name, optional operator family, access method, data type, and a list of operators and functions that make up the class definition.

## Parameters / Member Variables
- `type`: NodeTag identifier for this parse node type
- `opclassname`: List of String nodes representing the qualified name of the operator class being created
- `opfamilyname`: List of String nodes for the operator family name, or NIL if not specified (defaults to same as opclass name)
- `amname`: Character string specifying the name of the index access method this operator class is designed for
- `datatype`: TypeName pointer specifying the data type that this operator class handles
- `items`: List of CreateOpClassItem nodes defining the operators, functions, and storage parameters for the class
- `isDefault`: Boolean flag indicating whether this operator class should be marked as the default for its data type

## Dependencies
- Functions called/Symbols referenced:
  - TypeName (for data type specification)
- Called from (representative examples):
  - EventTriggerCollectCreateOpClass
  - DefineOpClass
  - ProcessUtilitySlow
  - DEFREM_H
  - CALLED_AS_EVENT_TRIGGER

## Notes and Other Information
- Part of PostgreSQL parse tree node hierarchy, inheriting from Node via NodeTag
- Used to implement CREATE OPERATOR CLASS functionality for defining custom indexing strategies
- Operator classes are essential for enabling indexes on user-defined data types
- The items list contains CreateOpClassItem structures defining operators (like <, =, >) and support functions needed by the access method
- If no operator family is specified, PostgreSQL creates one with the same name as the operator class
- The isDefault flag allows the new operator class to become the default choice for its data type
- Located in src/include/nodes/parsenodes.h:3165-3178