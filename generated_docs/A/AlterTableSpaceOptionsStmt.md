# AlterTableSpaceOptionsStmt

## Location
src/include/nodes/parsenodes.h: 2796 - 2802

## Overview
AlterTableSpaceOptionsStmt represents the parsed structure for an ALTER TABLESPACE SET/RESET options statement, used to modify storage options for an existing tablespace.

## Definition


## Detailed Description
This structure is part of PostgreSQL's parse tree node system and represents the ALTER TABLESPACE command when it's used to modify tablespace options. The statement allows database administrators to change storage-related parameters for tablespaces, such as random_page_cost, seq_page_cost, or other tablespace-specific options. The options can either be set to new values or reset to their defaults.

The structure is processed during query execution to modify the pg_tablespace system catalog, specifically updating the spcoptions column which stores tablespace-specific options as a text array.

## Parameters / Member Variables
- : NodeTag identifier indicating this is an AlterTableSpaceOptionsStmt node
- : Name of the tablespace whose options are to be modified
- : List of DefElem structures representing the options to set or modify
- : Boolean flag indicating whether to reset options to defaults (true) or set new values (false)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from node system)
  - List (from PostgreSQL's list implementation)
- Called from (representative examples):
  - AlterTableSpaceOptions (main execution function)
  - standard_ProcessUtility (utility command processor)

## Notes and Other Information
- This statement type is created during SQL parsing when ALTER TABLESPACE ... SET/RESET is encountered
- The actual option processing and validation occurs in the AlterTableSpaceOptions function in tablespace.c
- Options are validated against the tablespace_reloptions function to ensure they are valid tablespace parameters
- Requires ownership privileges on the target tablespace to execute
- Changes are immediately visible and affect future operations on the tablespace