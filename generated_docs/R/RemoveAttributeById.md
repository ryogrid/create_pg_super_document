# RemoveAttributeById

## Location
[src/backend/catalog/heap.c:1666-1766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1666-L1766)

## Overview
RemoveAttributeById is the core function for ALTER TABLE DROP COLUMN operations that marks an attribute as deleted in the pg_attribute system catalog and removes associated statistical entries.

## Definition

```c
void
RemoveAttributeById(Oid relid, AttrNumber attnum)
```
## Detailed Description
This function implements the guts of ALTER TABLE DROP COLUMN by actually marking the specified attribute as deleted in pg_attribute. It performs several critical operations: acquiring an exclusive lock on the target relation, marking the attribute as dropped, invalidating the type OID, removing not-null constraints, clearing generated column information, renaming the column to avoid conflicts, clearing missing values, and removing statistical data. The function ensures that the attribute becomes inaccessible while preserving essential type information (typlen and typalign) needed for tuple processing. Other cleanup tasks like removing pg_attrdef entries are handled by the dependency system.

## Parameters / Member Variables
- `relid`: OID of the relation containing the attribute to be dropped
- `attnum`: Attribute number (column number) of the attribute to be removed

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - SearchSysCacheCopy2
  - namestrcpy
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [RemoveStatistics](RemoveStatistics.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md)

## Notes and Other Information
- Acquires AccessExclusiveLock on the target relation which is held until end of transaction
- Changes column name to "........pg.dropped.N........" format to avoid naming conflicts
- Sets atttypid to InvalidOid but preserves attlen and attalign for tuple processing
- Clears atthasmissing flag and nullifies the attmissingval field
- Removes statistical entries via RemoveStatistics
- Triggers relcache flush automatically when pg_attribute is updated
- Works in conjunction with dependency.c for complete column removal