# AlterExtensionStmt

## Location
[src/include/nodes/parsenodes.h:2828-2833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2828-L2833)

## Overview
AlterExtensionStmt represents the parsed structure for an ALTER EXTENSION statement, currently used specifically for UPDATE operations to upgrade extensions to newer versions.

## Definition

```c
typedef struct AlterExtensionStmt
{
	NodeTag		type;
	char	   *extname;
	List	   *options;		/* List of DefElem nodes */
} AlterExtensionStmt;
```
## Detailed Description
This structure represents the ALTER EXTENSION SQL command, which is primarily used to update extensions to newer versions. The statement reads the current version from the pg_extension catalog, determines the target version (either specified explicitly or from the extension's default version), and identifies the sequence of update scripts needed to migrate from the current version to the target version.

The update process involves reading extension control files and executing SQL update scripts in the correct order to transform the extension from its current state to the desired version. The structure is currently limited to UPDATE operations, as indicated by the comment, but may be extended in the future to support other ALTER EXTENSION actions.

## Parameters / Member Variables
- : NodeTag identifier indicating this is an AlterExtensionStmt node
- : Name of the extension to be altered/updated
- : List of DefElem structures containing options for the operation (primarily 'new_version' for UPDATE operations)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from node system)
  - [List](../L/List.md) (from PostgreSQL's list implementation)
  - [DefElem](../D/DefElem.md) (for option specification)
- Called from (representative examples):
  - [ExecAlterExtensionStmt](../E/ExecAlterExtensionStmt.md) (main execution function)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processor)

## Notes and Other Information
- Currently only supports ALTER EXTENSION UPDATE operations
- The primary option is 'new_version' to specify the target version for updates
- If no version is specified, the extension's default_version from the control file is used
- The operation requires ownership of the extension
- Nested ALTER EXTENSION commands are not supported
- The system automatically identifies and executes the necessary update scripts to migrate between versions
- If the extension is already at the target version, a NOTICE is issued and no action is taken
- The update process is atomic and updates both the pg_extension catalog and applies all required schema changes