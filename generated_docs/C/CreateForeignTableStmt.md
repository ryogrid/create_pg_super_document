# CreateForeignTableStmt

## Location
src/include/nodes/parsenodes.h: 2895 - 2900

## Overview
CreateForeignTableStmt represents the parsed representation of a CREATE FOREIGN TABLE SQL statement, used to create a foreign table that provides access to external data through a foreign server.

## Definition

```c
typedef struct CreateForeignTableStmt
{
	CreateStmt	base;
	char	   *servername;
	List	   *options;
} CreateForeignTableStmt;
```
## Detailed Description
CreateForeignTableStmt is a specialized parse tree node that extends the standard CreateStmt structure to support foreign table creation. Foreign tables are virtual tables that provide a PostgreSQL interface to external data sources through foreign data wrappers and foreign servers. This structure inherits most table creation functionality from CreateStmt while adding foreign-table-specific elements.

The base CreateStmt contains standard table definition elements like column definitions, constraints, and table options, while the additional fields specify the connection to external data through a named foreign server and foreign-table-specific options.

## Parameters / Member Variables
- : CreateStmt structure containing standard table creation elements (columns, constraints, etc.)
- : Name of the foreign server that will provide access to the external data
- : List of DefElem nodes containing foreign-table-specific configuration options

## Dependencies
- Functions called/Symbols referenced:
  - CreateStmt (base structure for table creation)
  - List (PostgreSQL's list data structure)
- Called from (representative examples):
  - CreateForeignTable (src/backend/commands/foreigncmds.c:1415)
  - ImportForeignSchema (src/backend/commands/foreigncmds.c:1562, 1569)
  - transformCreateStmt (src/backend/parser/parse_utilcmd.c:227)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1195, 1197)

## Notes and Other Information
- This structure is defined in src/include/nodes/parsenodes.h in the foreign table statements section
- The structure uses inheritance-style composition with CreateStmt as the base
- The servername must reference an existing foreign server
- Foreign table options typically include mapping information for external data source schemas
- Used by both explicit CREATE FOREIGN TABLE commands and IMPORT FOREIGN SCHEMA operations
- Referenced by DEFREM_H header file as part of the definition/command processing interface
- More extensively referenced than other FDW structures, indicating its central role in foreign table operations