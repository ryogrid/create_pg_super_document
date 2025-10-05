# SetIndexStorageProperties

## Location
[src/backend/commands/tablecmds.c:8824-8886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8824-L8886)

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
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - [SearchSysCacheCopyAttNum](SearchSysCacheCopyAttNum.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecSetStorage](../A/ATExecSetStorage.md)
  - [ATExecSetCompression](../A/ATExecSetCompression.md)

## Notes and Other Information
- The function only updates index columns that directly correspond to the specified table column
- Uses the index key array to map table column numbers to index column positions
- Properly handles cases where the table column is not present in a particular index
- Maintains proper locking by opening and closing each index with the specified lock mode
- Invokes post-alter hooks to notify other subsystems of the changes
- Works with both storage and compression properties independently based on the boolean flags

## Simplified Source

```c
static void SetIndexStorageProperties(Relation rel, Relation attrelation,
                                    AttrNumber attnum,
                                    bool setstorage, char newstorage,
                                    bool setcompression, char newcompression,
                                    LOCKMODE lockmode) {
    // Iterate through all indexes on this table
    foreach(lc, RelationGetIndexList(rel)) {
        Oid indexoid = lfirst_oid(lc);
        Relation indrel = index_open(indexoid, lockmode);
        AttrNumber indattnum = 0;

        // Find the index column that corresponds to our table column
        for (int i = 0; i < indrel->rd_index->indnatts; i++) {
            if (indrel->rd_index->indkey.values[i] == attnum) {
                indattnum = i + 1;
                break;
            }
        }

        // Skip this index if it doesn't include our column
        if (indattnum == 0) {
            index_close(indrel, lockmode);
            continue;
        }

        // Get the index column's attribute tuple
        HeapTuple tuple = SearchSysCacheCopyAttNum(RelationGetRelid(indrel),
                                                  indattnum);

        if (HeapTupleIsValid(tuple)) {
            Form_pg_attribute attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

            // Update storage and/or compression properties
            if (setstorage)
                attrtuple->attstorage = newstorage;
            if (setcompression)
                attrtuple->attcompression = newcompression;

            // Save changes to catalog
            CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

            // Notify other subsystems of the change
            InvokeObjectPostAlterHook(RelationRelationId,
                                    RelationGetRelid(rel),
                                    attrtuple->attnum);

            heap_freetuple(tuple);
        }

        index_close(indrel, lockmode);
    }
}
```