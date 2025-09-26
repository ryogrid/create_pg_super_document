# AlterObjectDependsStmt

## Location
[src/include/nodes/parsenodes.h:3543-3551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3543-L3551)

## Overview
AlterObjectDependsStmt is a PostgreSQL parse node structure that represents an ALTER object DEPENDS ON EXTENSION statement for managing extension dependencies of database objects.

## Definition

```c
typedef struct AlterObjectDependsStmt
{
	NodeTag		type;
	ObjectType	objectType;		/* OBJECT_FUNCTION, OBJECT_TRIGGER, etc */
	RangeVar   *relation;		/* in case a table is involved */
	Node	   *object;			/* name of the object */
	String	   *extname;		/* extension name */
	bool		remove;			/* set true to remove dep rather than add */
} AlterObjectDependsStmt;
```
## Detailed Description
AlterObjectDependsStmt represents SQL statements that establish or remove dependencies between database objects and extensions. This allows objects to be marked as belonging to an extension, which affects their lifecycle - when the extension is dropped, dependent objects are automatically dropped as well. The structure supports both adding dependencies (when remove is false) and removing them (when remove is true).

## Parameters / Member Variables
- `type`: Standard NodeTag for parse tree identification
- `objectType`: Specifies the type of object (OBJECT_FUNCTION, OBJECT_TRIGGER, etc.)
- `*relation`: RangeVar pointer used when the object is associated with a table or relation
- `*object`: Generic Node pointer containing the name/identifier of the target object
- `*extname`: String containing the name of the extension
- `remove`: Boolean flag - true to remove the dependency, false to add it
## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration for database object types)
  - [RangeVar](../R/RangeVar.md) (structure for table/relation references)
  - [String](../S/String.md) (PostgreSQL string node type)
  - NodeTag (standard parse node identification)
- Called from (representative examples):
  - [ExecAlterObjectDependsStmt](../E/ExecAlterObjectDependsStmt.md) (main execution function)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
This statement type is crucial for extension management in PostgreSQL, allowing fine-grained control over which objects belong to which extensions. It supports the SQL standard ALTER ... DEPENDS ON EXTENSION syntax and enables proper cleanup when extensions are dropped. The remove flag provides bidirectional functionality, allowing both the establishment and removal of extension dependencies.