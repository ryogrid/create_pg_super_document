# AlterFdwStmt

## Location
src/include/nodes/parsenodes.h: 2857 - 2863

## Overview
AlterFdwStmt represents the parsed representation of an ALTER FOREIGN DATA WRAPPER SQL statement, used to modify an existing foreign data wrapper definition in PostgreSQL.

## Definition


## Detailed Description
AlterFdwStmt is a parse tree node that encapsulates the information needed to alter an existing foreign data wrapper. This structure stores the parsed components of the ALTER FOREIGN DATA WRAPPER command, allowing modification of handler/validator functions and configuration options of an already created FDW.

The structure maintains the same basic format as CreateFdwStmt but is used specifically for modification operations. It enables users to update FDW configurations without recreating the entire wrapper, which is particularly useful for adjusting connection parameters or changing handler functions.

## Parameters / Member Variables
- : NodeTag identifier marking this as an AlterFdwStmt node in the parse tree
- : The name of the existing foreign data wrapper to be modified
- : List of DefElem nodes specifying updated HANDLER and VALIDATOR function options
- : List of DefElem nodes containing updated generic configuration options for the FDW

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - List (PostgreSQL's list data structure)
- Called from (representative examples):
  - AlterForeignDataWrapper (src/backend/commands/foreigncmds.c:685)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1591)

## Notes and Other Information
- This structure is defined in src/include/nodes/parsenodes.h immediately after CreateFdwStmt
- The func_options and options lists can contain SET, ADD, or DROP operations for modifying FDW properties
- Unlike CREATE operations, ALTER operations must reference an existing FDW name
- Referenced by DEFREM_H header file as part of the definition/command processing interface