# AlterTableCmd

## Location
[src/include/nodes/parsenodes.h:2426-2440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2426-L2440)

## Overview
AlterTableCmd represents a single subcommand within an ALTER TABLE statement, defining one specific alteration operation to be performed on a table.

## Definition

```c
typedef struct AlterTableCmd	/* one subcommand of an ALTER TABLE */
{
	NodeTag		type;
	AlterTableType subtype;		/* Type of table alteration to apply */
	char	   *name;			/* column, constraint, or trigger to act on,
								 * or tablespace, access method */
	int16		num;			/* attribute number for columns referenced by
								 * number */
	RoleSpec   *newowner;
	Node	   *def;			/* definition of new column, index,
								 * constraint, or parent table */
	DropBehavior behavior;		/* RESTRICT or CASCADE for DROP cases */
	bool		missing_ok;		/* skip error if missing? */
	bool		recurse;		/* exec-time recursion */
} AlterTableCmd;
```
## Detailed Description
AlterTableCmd is a fundamental data structure in PostgreSQL's DDL (Data Definition Language) processing system. It encapsulates a single alteration operation within a potentially complex ALTER TABLE statement. Each ALTER TABLE command can contain multiple subcommands, and each subcommand is represented by one AlterTableCmd structure.

The structure provides a flexible framework for representing various types of table alterations, from adding/dropping columns to modifying constraints, changing ownership, or altering table properties. The design allows the ALTER TABLE infrastructure to process different types of operations uniformly while maintaining type-specific information in the subtype and auxiliary fields.

## Parameters / Member Variables
- : NodeTag for node type identification in PostgreSQL's node system
- : AlterTableType enum specifying the exact type of alteration (ADD_COLUMN, DROP_COLUMN, ALTER_COLUMN_TYPE, etc.)
- : String identifier for the target object (column name, constraint name, trigger name, tablespace name, or access method name)
- : Attribute number used when referencing columns by position rather than name
- : RoleSpec structure defining the new owner for ownership change operations
- : Generic Node pointer containing the definition data for new objects (column definitions, constraint definitions, etc.)
- : DropBehavior enum (RESTRICT or CASCADE) controlling how dependent objects are handled during DROP operations
- : Boolean flag indicating whether to skip errors if the target object doesn't exist (IF EXISTS semantics)
- : Boolean flag controlling whether the operation should be applied recursively to child tables

## Dependencies
- Functions called/Symbols referenced:
  - [AlterTableType](AlterTableType.md)
  - [RoleSpec](../R/RoleSpec.md)
  - DropBehavior
- Called from (representative examples):
  - [ATController](ATController.md)
  - [ATPrepCmd](ATPrepCmd.md)
  - [ATExecCmd](ATExecCmd.md)
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Part of PostgreSQL's parse tree node system, inheriting from the standard Node structure
- Used extensively throughout the table alteration infrastructure in src/backend/commands/tablecmds.c
- The structure design supports PostgreSQL's multi-phase ALTER TABLE execution model
- Different subcommands may use different subsets of the member variables depending on their specific requirements
- Critical for event trigger support, allowing detailed tracking of individual ALTER TABLE operations