# CreateSharedComments

## Location
[src/backend/commands/comment.c:238-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L238-L325)

## Overview
Creates, updates, or deletes comments for cluster-wide shared objects by manipulating the pg_shdescription system catalog table.

## Definition


## Detailed Description
CreateSharedComments manages comments for cluster-wide shared objects (databases, tablespaces, roles) in the pg_shdescription catalog table. It operates similarly to CreateComments but works with shared objects that exist across the entire database cluster rather than within a single database. The function performs insert, update, or delete operations based on the comment parameter, using a two-key lookup system (object OID and class OID) since shared objects don't have sub-objects.

Like CreateComments, it treats empty strings as NULL comments for deletion, and uses the standard catalog interface functions to maintain MVCC consistency.

## Parameters / Member Variables
- : Object identifier of the target shared object (database, tablespace, or role OID)
- : OID of the system catalog containing the shared object (e.g., DatabaseRelationId, TableSpaceRelationId, AuthIdRelationId)
- : Comment text to store, or NULL to delete existing comment

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the pg_shdescription relation for modification
  - [systable_beginscan](../s/systable_beginscan.md): Initiates indexed scan using SharedDescriptionObjIndexId
  - [systable_getnext](../s/systable_getnext.md): Retrieves matching tuples from the scan
  - [CatalogTupleDelete](CatalogTupleDelete.md): Removes existing comment tuple
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates updated tuple with new comment
  - [CatalogTupleUpdate](CatalogTupleUpdate.md): Updates existing tuple in catalog
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates new tuple for insertion
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts new comment tuple
  - [heap_freetuple](../h/heap_freetuple.md): Frees allocated tuple memory
- Called from (representative examples):
  - [CommentObject](CommentObject.md): Routes shared object comments to this function

## Notes and Other Information
- Uses SharedDescriptionObjIndexId for efficient lookups by (objoid, classoid) 
- Only handles cluster-wide objects: databases, tablespaces, and roles
- Does not use sub-object IDs since shared objects don't have sub-components
- Acquires RowExclusiveLock on pg_shdescription to prevent concurrent modifications
- Follows same empty-string-to-NULL normalization as CreateComments
- Memory management includes proper cleanup of temporary heap tuples