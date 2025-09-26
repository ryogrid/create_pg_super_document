# AlterForeignServerStmt

## Location
[src/include/nodes/parsenodes.h:2881-2888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2881-L2888)

## Overview
AlterForeignServerStmt represents the parsed representation of an ALTER FOREIGN SERVER SQL statement, used to modify configuration properties of an existing foreign server.

## Definition

```c
typedef struct AlterForeignServerStmt
{
	NodeTag		type;
	char	   *servername;		/* server name */
	char	   *version;		/* optional server version */
	List	   *options;		/* generic options to server */
	bool		has_version;	/* version specified */
} AlterForeignServerStmt;
```
## Detailed Description
AlterForeignServerStmt is a parse tree node that encapsulates the information needed to alter an existing foreign server. This structure allows modification of server configuration without recreating the server or affecting dependent foreign tables. The structure is more streamlined than its CREATE counterpart, focusing on the modifiable aspects of a foreign server.

The has_version boolean field explicitly tracks whether a version was specified in the ALTER command, distinguishing between setting a version to NULL versus not specifying a version at all. This enables precise control over version updates.

## Parameters / Member Variables
- : NodeTag identifier marking this as an AlterForeignServerStmt node in the parse tree
- : The name of the existing foreign server to be modified
- : Optional new version string for the external server
- : List of DefElem nodes containing updated server-specific configuration options
- : Boolean flag indicating whether a version specification was provided

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - [List](../L/List.md) (PostgreSQL's list data structure)
- Called from (representative examples):
  - [AlterForeignServer](AlterForeignServer.md) (src/backend/commands/foreigncmds.c:985)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1599)

## Notes and Other Information
- This structure is defined in src/include/nodes/parsenodes.h following CreateForeignServerStmt
- Unlike CREATE operations, the servertype and fdwname cannot be altered and are not included
- The has_version field enables distinction between explicit NULL version setting and no version change
- The options list can contain SET, ADD, or DROP operations for modifying server properties
- Referenced by DEFREM_H header file as part of the definition/command processing interface