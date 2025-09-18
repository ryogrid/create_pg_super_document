# SetIndexStorageProperties

## Location
src/backend/commands/tablecmds.c: 8824 - 8886

## Overview
SetIndexStorageProperties is a helper function that propagates storage and compression property changes from a table column to corresponding index columns that reference that table column.

## Definition
static void SetIndexStorageProperties(Relation rel, Relation attrelation, AttrNumber attnum, bool setstorage, char newstorage, bool setcompression, char newcompression, LOCKMODE lockmode)

## Detailed Description
This helper function ensures that when storage or compression properties are changed on a table column, those changes are automatically propagated to any index columns that directly reference the table column. It iterates through all indexes associated with the relation, identifies which index columns correspond to the specified table column number, and updates their attstorage and/or attcompression fields accordingly. This maintains consistency between table columns and their corresponding index columns regarding storage and compression settings.

The function handles the complex task of mapping table column numbers to index column positions and only updates indexes where the specified table column is actually indexed.

## Parameters / Member Variables
- : The relation (table) whose column properties are being changed
- : The already-opened pg_attribute relation for catalog updates
- : The attribute number of the table column being modified
- : Boolean flag indicating whether to update storage properties
- : The new storage type character (if setstorage is true)
- : Boolean flag indicating whether to update compression properties
- : The new compression method character (if setcompression is true)
- : The lock mode to use when accessing indexes

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - index_open
  - index_close
  - SearchSysCacheCopyAttNum
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - heap_freetuple
- Called from (representative examples):
  - ATExecSetStorage
  - ATExecSetCompression

## Notes and Other Information
- The function only updates index columns that directly correspond to the specified table column
- Uses the index key array to map table column numbers to index column positions
- Properly handles cases where the table column is not present in a particular index
- Maintains proper locking by opening and closing each index with the specified lock mode
- Invokes post-alter hooks to notify other subsystems of the changes
- Works with both storage and compression properties independently based on the boolean flags