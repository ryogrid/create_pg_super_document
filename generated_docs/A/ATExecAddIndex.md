# ATExecAddIndex

## Location
src/backend/commands/tablecmds.c: 9179 - 9241

## Overview
ATExecAddIndex implements the execution of index creation during ALTER TABLE operations, specifically handling constraints that are converted to index creation commands by the parser.

## Definition


## Detailed Description
This function creates indexes as part of ALTER TABLE processing, particularly for UNIQUE and PRIMARY KEY constraints that are internally converted to AT_AddIndex subcommands by parse_utilcmd.c. It coordinates with the ALTER TABLE infrastructure to determine appropriate timing for index creation, handles index rebuilding scenarios, and manages storage reuse when rebuilding existing indexes. The function delegates the actual index creation to DefineIndex but provides ALTER TABLE-specific context and options.

The function supports both new index creation and rebuilding scenarios, with special handling for reusing storage from previously dropped indexes during concurrent operations.

## Parameters / Member Variables
- `tab`: Information about the table being altered, including rewrite status
- `rel`: The relation (table) on which to create the index
- `stmt`: The index statement containing index definition details
- `is_rebuild`: Flag indicating whether this is rebuilding an existing index
- `lockmode`: The lock mode to use (though not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [DefineIndex](../D/DefineIndex.md)
  - RelFileNumberIsValid
  - [index_open](../i/index_open.md)
  - [RelationPreserveStorage](../R/RelationPreserveStorage.md)
  - [index_close](../i/index_close.md)
  - [AlteredTableInfo](AlteredTableInfo.md) (struct)
  - IndexStmt (struct)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (multiple call sites)
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:9179-9241
- Returns ObjectAddress of the created index
- No direct grammar command exists - generated internally from constraints
- Asserts that the IndexStmt is already transformed and not concurrent
- Suppresses schema rights checks when rebuilding existing indexes
- May skip index build phase if table rewrite will handle it (phase 3)
- Handles storage reuse via oldNumber field in IndexStmt for rebuilds
- Cancels pending storage deletion when reusing existing index storage
- Restores createSubid and firstRelfilelocatorSubid fields for reused indexes
- Uses InvalidOid for predefined OID, parent index, and parent constraint
- Part of the broader ALTER TABLE command execution infrastructure