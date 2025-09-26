# DropStmt

## Location
src/include/nodes/parsenodes.h: 3226 - 3234

## Overview
DropStmt represents various DROP statements in the PostgreSQL parser, providing a unified structure for dropping tables, sequences, views, indexes, types, domains, conversions, schemas, and other database objects.

## Definition

```c
typedef struct DropStmt
{
	NodeTag		type;
	List	   *objects;		/* list of names */
	ObjectType	removeType;		/* object type */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if object is missing? */
	bool		concurrent;		/* drop index concurrently? */
} DropStmt;
```
## Detailed Description
DropStmt is a parse tree node that represents DROP statements for various database objects. It provides a unified interface for dropping different types of objects including tables, sequences, views, indexes, types, domains, conversions, and schemas. The structure supports both simple drops and cascading drops that remove dependent objects.

The removeType field specifies what kind of object is being dropped using the ObjectType enumeration, which includes values like OBJECT_TABLE, OBJECT_SEQUENCE, OBJECT_VIEW, OBJECT_INDEX, OBJECT_TYPE, OBJECT_DOMAIN, OBJECT_CONVERSION, OBJECT_SCHEMA, and many others. The behavior field controls whether the drop should fail if dependent objects exist (RESTRICT) or should cascade to remove dependent objects as well (CASCADE).

Special handling is provided for index drops through the concurrent flag, which enables DROP INDEX CONCURRENTLY operations that don't block concurrent operations on the table.

## Parameters / Member Variables
- : NodeTag identifier for this parse tree node type
- : List of object names to be dropped (can be qualified names for schema.object notation)
- : ObjectType enum specifying the type of objects being dropped (OBJECT_TABLE, OBJECT_INDEX, etc.)
- : DropBehavior enum controlling dependency handling (DROP_RESTRICT or DROP_CASCADE)
- : Boolean flag for IF EXISTS behavior - when true, no error is raised if the object doesn't exist
- : Boolean flag for DROP INDEX CONCURRENTLY - allows concurrent operations during index removal

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType
  - DropBehavior
- Called from (representative examples):
  - RemoveObjects
  - RemoveRelations
  - standard_ProcessUtility
  - ProcessUtilitySlow
  - ExecDropStmt
  - CreateCommandTag

## Notes and Other Information
- This single structure handles multiple DROP statement variants, making the parser more uniform
- The concurrent flag is specifically for index drops and is ignored for other object types
- CASCADE behavior can lead to dropping more objects than explicitly specified in the objects list
- The missing_ok flag implements IF EXISTS semantics, making scripts more robust
- Objects list can contain multiple names for dropping several objects in one statement (e.g., DROP TABLE t1, t2, t3)
- Qualified names in the objects list allow dropping objects from specific schemas
- The structure is processed by different handler functions depending on the removeType