# index_insert

## Location
src/backend/access/index/indexam.c: 213 - 240

## Overview
Inserts an index tuple into an index relation using the access method's insert procedure.

## Definition
```c
bool index_insert(Relation indexRelation,
                 Datum *values,
                 bool *isnull,
                 ItemPointer heap_t_ctid,
                 Relation heapRelation,
                 IndexUniqueCheck checkUnique,
                 bool indexUnchanged,
                 IndexInfo *indexInfo)
```

## Detailed Description
The `index_insert` function provides a generic interface for inserting tuples into any type of index. It delegates the actual insertion to the appropriate access method's `aminsert` function while performing necessary validations and conflict checks.

The function first performs relation checks and verifies that the access method has an insert procedure. For indexes that don't support predicate locking (when `ampredlocks` is false), it checks for serializable conflicts. Finally, it calls the index access method's specific insert function to perform the actual insertion.

The function returns a boolean indicating whether the insertion was successful.

## Parameters
- `indexRelation`: The index relation to insert into
- `values`: Array of Datum values for the index tuple
- `isnull`: Array of boolean flags indicating which values are NULL
- `heap_t_ctid`: Item pointer to the heap tuple being indexed
- `heapRelation`: The heap relation containing the tuple being indexed
- `checkUnique`: Specifies the level of uniqueness checking to perform
- `indexUnchanged`: Whether the index values are unchanged (optimization hint)
- `indexInfo`: Metadata about the index structure and properties

## Dependencies
- Functions called/Symbols referenced:
  - IndexUniqueCheck (type)
  - IndexInfo (type)
  - RELATION_CHECKS (macro)
  - CHECK_REL_PROCEDURE (macro)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
- Called from (representative examples):
  - [toast_save_datum](../t/toast_save_datum.md)
  - [heapam_index_validate_scan](../h/heapam_index_validate_scan.md)
  - [CatalogIndexInsert](../C/CatalogIndexInsert.md)
  - [ExecInsertIndexTuples](../E/ExecInsertIndexTuples.md)

## Notes and Other Information
- This is a generic interface that works with all index access methods
- Performs serializable conflict checking for indexes that don't support predicate locks
- Returns a boolean success indicator from the underlying access method
- The actual insertion logic is implemented by each specific index access method
- Used throughout PostgreSQL for all index insertions regardless of index type
- Located in src/backend/access/index/indexam.c:213-240