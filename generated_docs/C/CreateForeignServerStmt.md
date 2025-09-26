# CreateForeignServerStmt

## Location
[src/include/nodes/parsenodes.h:2870-2879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2870-L2879)

## Overview
CreateForeignServerStmt represents the parsed representation of a CREATE FOREIGN SERVER SQL statement, used to create a new foreign server definition that connects to an external data source through a foreign data wrapper.

## Definition

```c
typedef struct CreateForeignServerStmt
{
	NodeTag		type;
	char	   *servername;		/* server name */
	char	   *servertype;		/* optional server type */
	char	   *version;		/* optional server version */
	char	   *fdwname;		/* FDW name */
	bool		if_not_exists;	/* just do nothing if it already exists? */
	List	   *options;		/* generic options to server */
} CreateForeignServerStmt;
```
## Detailed Description
CreateForeignServerStmt is a parse tree node that encapsulates the information needed to create a foreign server. A foreign server represents a connection to an external data source and is associated with a specific foreign data wrapper. This structure stores all the parsed components of the CREATE FOREIGN SERVER command, including server identification, connection parameters, and configuration options.

The structure supports the IF NOT EXISTS clause through the if_not_exists boolean field, allowing for conditional server creation. The server acts as an intermediate layer between the FDW and foreign tables, providing connection and configuration details specific to a particular external data source instance.

## Parameters / Member Variables
- : NodeTag identifier marking this as a CreateForeignServerStmt node in the parse tree
- : The name of the foreign server to be created
- : Optional server type specification for documentation purposes
- : Optional version string for the external server
- : Name of the foreign data wrapper this server will use
- : Boolean flag indicating whether to skip creation if server already exists
- : List of DefElem nodes containing server-specific configuration options

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - [List](../L/List.md) (PostgreSQL's list data structure)
- Called from (representative examples):
  - [CreateForeignServer](CreateForeignServer.md) (src/backend/commands/foreigncmds.c:849)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1595)

## Notes and Other Information
- This structure is defined in src/include/nodes/parsenodes.h in the foreign server statements section
- The servertype and version fields are optional and primarily serve documentation purposes
- The fdwname must reference an existing foreign data wrapper
- The options list typically contains connection parameters specific to the external data source
- Referenced by DEFREM_H header file as part of the definition/command processing interface