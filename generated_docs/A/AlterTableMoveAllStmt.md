# AlterTableMoveAllStmt

## Location
[src/include/nodes/parsenodes.h:2804-2812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2804-L2812)

## Overview
AlterTableMoveAllStmt represents the parsed structure for an ALTER TABLE/INDEX/MATERIALIZED VIEW ALL IN TABLESPACE ... SET TABLESPACE statement, used to move all objects of a specific type from one tablespace to another.

## Definition

```c
typedef struct AlterTableMoveAllStmt
{
	NodeTag		type;
	char	   *orig_tablespacename;
	ObjectType	objtype;		/* Object type to move */
	List	   *roles;			/* List of roles to move objects of */
	char	   *new_tablespacename;
	bool		nowait;
} AlterTableMoveAllStmt;
```
## Detailed Description
This structure represents a bulk tablespace move operation that allows moving all objects of a specific type (tables, indexes, or materialized views) from one tablespace to another in a single command. The operation can be optionally filtered by object ownership, allowing users to move only objects owned by specific roles. This is particularly useful for tablespace maintenance, reorganization, or migration scenarios.

The statement provides a NOWAIT option to avoid blocking if any required locks cannot be immediately acquired. The operation scans the pg_class system catalog to find all matching objects and moves them one by one using the standard ALTER TABLE SET TABLESPACE mechanism.

## Parameters / Member Variables
- : NodeTag identifier indicating this is an AlterTableMoveAllStmt node
- : Name of the source tablespace from which objects will be moved
- : Type of database objects to move (OBJECT_TABLE, OBJECT_INDEX, or OBJECT_MATVIEW)
- : Optional list of role specifications to filter objects by ownership (NULL means all owners)
- : Name of the destination tablespace where objects will be moved
- : Boolean flag indicating whether to fail immediately if locks cannot be acquired (true) or wait for locks (false)

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration defining object types)
  - NodeTag (from node system)
  - [List](../L/List.md) (from PostgreSQL's list implementation)
- Called from (representative examples):
  - [AlterTableMoveAll](AlterTableMoveAll.md) (main execution function)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processor)
  - [CreateCommandTag](../C/CreateCommandTag.md) (for command tag generation)

## Notes and Other Information
- This command requires CREATE privileges on the destination tablespace
- Objects in pg_catalog, shared tables, temporary tables, and TOAST tables are automatically excluded from the move operation
- The operation acquires AccessExclusiveLock on all objects being moved
- Permission checks ensure the user owns all objects being moved
- If no matching objects are found, a NOTICE is issued rather than an error
- The operation is atomic - all objects are locked first, then moved together
- Supports tables, indexes, and materialized views but not other object types like sequences or functions