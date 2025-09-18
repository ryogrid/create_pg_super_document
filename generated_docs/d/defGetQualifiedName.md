# defGetQualifiedName

## Location
[src/backend/commands/define.c:252-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/define.c#L252-L283)

## Overview
Extracts a possibly-qualified name as a List of Strings from a DefElem structure, supporting multiple input formats for backward compatibility.

## Definition


## Detailed Description
The  function is a utility function that extracts qualified names from DefElem structures in PostgreSQL's DDL command processing. A qualified name can be a simple name like "myfunction" or a schema-qualified name like "myschema.myfunction". The function handles three different node types:

1. **T_TypeName**: Returns the names list from a TypeName node
2. **T_List**: Returns the list directly when already in the correct format
3. **T_String**: Wraps a single string in a list for backward compatibility with quoted names

This flexibility allows the function to handle various syntax forms that users might employ when specifying object names in DDL statements.

## Parameters / Member Variables
- : Pointer to a DefElem structure containing the definition element to extract the qualified name from

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type)
  - nodeTag (macro to get node type)
  - [TypeName](../T/TypeName.md) (structure type)
  - [List](../L/List.md) (structure type for linked lists)
  - list_make1 (function to create single-element list)
- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md) (aggregate definition commands)
  - [DefineCollation](../D/DefineCollation.md) (collation definition commands)
  - [DefineOperator](../D/DefineOperator.md) (operator definition commands)
  - [DefineType](../D/DefineType.md) (type definition commands)
  - Various other DDL command functions

## Notes and Other Information
- Returns NIL as a fallback to keep the compiler quiet, though this should never be reached due to error handling
- Provides backward compatibility by accepting quoted string names and converting them to lists
- Extensively used throughout PostgreSQL's DDL command processing infrastructure
- The qualified name format supports PostgreSQL's schema.object naming convention
- Located in src/backend/commands/define.c:252-283