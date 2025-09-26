# AlterTypeStmt

## Location
src/include/nodes/parsenodes.h: 3595 - 3600

## Overview
AlterTypeStmt represents the parsed structure of an ALTER TYPE SQL statement that allows modification of type properties through SET operations.

## Definition


## Detailed Description
AlterTypeStmt is a parse node that represents the ALTER TYPE SET statement in PostgreSQL's SQL grammar. This statement allows users to modify properties of existing types, particularly for customizing type behavior through operator-related definitions. The statement follows the syntax:  where the options are parsed into DefElem structures that specify what properties to modify.

## Parameters / Member Variables
- : Standard NodeTag identifying this as an AlterTypeStmt parse node
- : List representation of the type name, which may be schema-qualified (e.g., schema.typename)
- : List of DefElem nodes containing the property definitions to be set for the type

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - List (for typeName and options storage)
  - DefElem (for individual option specifications)
- Called from (representative examples):
  - AlterType (processes the statement in src/backend/commands/typecmds.c:4312)
  - ProcessUtilitySlow (dispatches the statement in src/backend/tcop/utility.c:1801)

## Notes and Other Information
- This statement is parsed in gram.y with the rule: 
- The options list contains DefElem structures that specify operator definitions and other type properties
- The statement requires appropriate permissions to modify the target type
- This is part of PostgreSQL's extensible type system allowing customization of user-defined types