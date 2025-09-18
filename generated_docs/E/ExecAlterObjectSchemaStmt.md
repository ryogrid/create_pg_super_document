# ExecAlterObjectSchemaStmt

## Location
[src/backend/commands/alter.c:521-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L521-L613)

## Overview
Executes an ALTER OBJECT SET SCHEMA statement by dispatching to the appropriate type-specific function based on the object type being altered.

## Definition


## Detailed Description
ExecAlterObjectSchemaStmt serves as the central dispatcher for ALTER OBJECT SET SCHEMA statements in PostgreSQL. It examines the object type specified in the statement and routes the operation to the appropriate specialized function for handling that particular object type. The function supports moving various database objects between schemas, including tables, functions, types, operators, and many others.

The function handles two main categories of objects:
1. **Special cases**: Extensions, tables/views/sequences, and domains/types have dedicated functions
2. **Generic path**: Most other objects (functions, operators, collations, etc.) use a common generic approach via 

For generic objects, the function resolves the object address, opens the appropriate system catalog, looks up the target namespace, and calls the internal namespace alteration function.

## Parameters / Member Variables
- : Pointer to the parsed ALTER OBJECT SET SCHEMA statement containing the object type, object identifier, and target schema name
- : Optional output parameter that receives the ObjectAddress of the original schema if not NULL

## Dependencies
- Functions called/Symbols referenced:
  - : For extension objects
  - : For table-like objects (tables, views, sequences, etc.)
  - : For type and domain objects  
  - : Generic object address resolution
  - : Target schema lookup
  - : Core namespace alteration logic
  - : Sets the old schema address output parameter
- Called from (representative examples):
  - : Main utility command processing
  - : Utility command processing path

## Notes and Other Information
- Returns the ObjectAddress of the altered object
- Handles a wide variety of object types through a switch statement
- Uses AccessExclusiveLock when resolving object addresses for schema changes
- The function maintains backward compatibility by optionally reporting the original schema
- Error handling includes an assertion that relation should be NULL for generic path objects
- Falls back to elog(ERROR) for unrecognized object types