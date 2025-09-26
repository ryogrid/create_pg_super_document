# ResetRelRewrite

## Location
[src/backend/commands/tablecmds.c:4228-4280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4228-L4280)

## Overview
Resets the relrewrite field in the pg_class system catalog to InvalidOid for a specified relation, effectively clearing any active table rewrite operation association.

## Definition
```c
void ResetRelRewrite(Oid myrelid)
```

## Detailed Description
This function is responsible for clearing the relrewrite field in the pg_class catalog table for a given relation. The relrewrite field typically contains the OID of a relation that is being used as a temporary storage during table rewrite operations (such as ALTER TABLE operations that require a full table rebuild). By setting this field to InvalidOid, the function indicates that no active rewrite operation is associated with the relation.

The function performs a direct update to the pg_class system catalog using proper locking mechanisms and cache invalidation procedures. It opens the relation catalog with RowExclusiveLock to ensure safe concurrent access, looks up the target relation's tuple, updates the relrewrite field, and properly closes the catalog.

## Parameters / Member Variables
- `myrelid`: The OID of the relation whose relrewrite field should be reset to InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens the pg_class catalog with proper locking)
  - SearchSysCacheCopy1 (searches system cache for the relation tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (updates the tuple in the catalog)
  - [heap_freetuple](../h/heap_freetuple.md) (frees the heap tuple memory)
  - [table_close](../t/table_close.md) (closes the catalog relation)
  - Form_pg_class (structure type for pg_class tuples)
- Called from (representative examples):
  - [finish_heap_swap](../f/finish_heap_swap.md) (in cluster.c:1612, called after completing a table rewrite operation)

## Notes and Other Information
- This function is typically called at the end of table rewrite operations to clean up the relrewrite association
- The function assumes the relation exists and will throw an ERROR if the cache lookup fails
- Uses RowExclusiveLock on the pg_class catalog to ensure safe concurrent modifications
- Part of the table command infrastructure that manages ALTER TABLE and similar DDL operations
- The relrewrite field is used during operations like CLUSTER, VACUUM FULL, and certain ALTER TABLE commands that require rebuilding the entire table