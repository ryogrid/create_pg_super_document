# RenameStmt

## Location
src/include/nodes/parsenodes.h: 3525 - 3537

## Overview
RenameStmt is a PostgreSQL parse node structure that represents an ALTER statement for renaming database objects such as tables, columns, constraints, triggers, and other schema elements.

## Definition

```c
typedef struct RenameStmt
{
	NodeTag		type;
	ObjectType	renameType;		/* OBJECT_TABLE, OBJECT_COLUMN, etc */
	ObjectType	relationType;	/* if column name, associated relation type */
	RangeVar   *relation;		/* in case it's a table */
	Node	   *object;			/* in case it's some other object */
	char	   *subname;		/* name of contained object (column, rule,
								 * trigger, etc) */
	char	   *newname;		/* the new name */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if missing? */
} RenameStmt;
```
## Detailed Description
RenameStmt is a parse tree node that encapsulates all the information needed to rename various PostgreSQL database objects. It supports renaming tables, columns, constraints, triggers, types, policies, and other schema objects. The structure provides flexibility to handle different object types through its renameType field and can optionally specify CASCADE or RESTRICT behavior for objects that have dependencies.

## Parameters / Member Variables
- : Standard NodeTag for parse tree identification
- : Specifies the type of object being renamed (OBJECT_TABLE, OBJECT_COLUMN, etc.)
- : When renaming a column, specifies the type of the containing relation
- : RangeVar pointer used when the object being renamed is a table or relation
- : Generic Node pointer for other types of objects being renamed
- : Name of the contained object (used for columns, rules, triggers within a relation)
- : The new name to assign to the object
- : Specifies CASCADE or RESTRICT behavior for handling dependencies
- : Boolean flag to suppress errors if the object doesn't exist (IF EXISTS semantics)

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration for object types)
  - RangeVar (structure for table/relation references)
  - DropBehavior (enumeration for CASCADE/RESTRICT behavior)
  - NodeTag (standard parse node identification)
- Called from (representative examples):
  - ExecRenameStmt (main execution function)
  - standard_ProcessUtility (utility command processing)
  - renameatt (attribute/column renaming)
  - RenameConstraint (constraint renaming)
  - RenameRelation (table/relation renaming)

## Notes and Other Information
The RenameStmt structure is used throughout PostgreSQL's DDL processing pipeline, from parsing through execution. It supports a wide variety of rename operations and includes safety features like missing_ok to handle IF EXISTS clauses and behavior settings to control dependency handling during renames.