# CreateFdwStmt

## Location
src/include/nodes/parsenodes.h: 2849 - 2855

## Overview
CreateFdwStmt represents the parsed representation of a CREATE FOREIGN DATA WRAPPER SQL statement, used to create a new foreign data wrapper definition in PostgreSQL.

## Definition


## Detailed Description
CreateFdwStmt is a parse tree node that encapsulates the information needed to create a foreign data wrapper. Foreign data wrappers are PostgreSQL extensions that enable access to external data sources as if they were regular PostgreSQL tables. This structure stores the parsed components of the CREATE FOREIGN DATA WRAPPER command, including the wrapper name, handler/validator function specifications, and generic configuration options.

The structure is part of PostgreSQL's parse tree infrastructure and is created during the parsing phase of SQL statement processing. It serves as an intermediate representation before the actual foreign data wrapper is created in the system catalogs.

## Parameters / Member Variables
- : NodeTag identifier marking this as a CreateFdwStmt node in the parse tree
- : The name of the foreign data wrapper to be created
- : List of DefElem nodes specifying HANDLER and VALIDATOR function options
- : List of DefElem nodes containing generic configuration options for the FDW

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - List (PostgreSQL's list data structure)
- Called from (representative examples):
  - CreateForeignDataWrapper (src/backend/commands/foreigncmds.c:569)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1587)

## Notes and Other Information
- This structure is defined in src/include/nodes/parsenodes.h alongside other DDL statement nodes
- The func_options list typically contains DefElem nodes with defnames like "handler" and "validator"
- The options list contains DefElem nodes representing WITH clause options from the SQL statement
- Referenced by DEFREM_H header file, indicating it's part of the definition/command processing interface