# DeleteSharedComments

## Location
src/backend/commands/comment.c: 374 - 409

## Overview
Removes comments for cluster-wide shared objects from the pg_shdescription catalog table.

## Definition


## Detailed Description
DeleteSharedComments removes comment entries from the pg_shdescription catalog table for cluster-wide shared objects such as databases, tablespaces, and roles. Unlike DeleteComments, this function operates on the shared description catalog and doesn't handle sub-object IDs since shared objects don't have sub-components. It performs a systematic scan using two-key lookups (object OID and class OID) and deletes all matching comment tuples.

The function is typically called during the dropping of shared objects to clean up their associated comments as part of the cascade deletion process.

## Parameters / Member Variables
- : Object identifier of the shared object whose comments should be deleted (database, tablespace, or role OID)
- : OID of the system catalog containing the shared object (e.g., DatabaseRelationId, TableSpaceRelationId, AuthIdRelationId)

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the pg_shdescription relation for modification
  - [systable_beginscan](../s/systable_beginscan.md): Initiates indexed scan using SharedDescriptionObjIndexId
  - [systable_getnext](../s/systable_getnext.md): Iterates through matching comment tuples
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Removes each matching comment tuple
  - [systable_endscan](../s/systable_endscan.md): Ends the systematic scan
  - table_close: Closes the pg_shdescription relation
- Called from (representative examples):
  - [dropdb](../d/dropdb.md): Removes database comments when dropping a database
  - [DropTableSpace](DropTableSpace.md): Removes tablespace comments when dropping a tablespace
  - [DropRole](DropRole.md): Removes role comments when dropping a user/role

## Notes and Other Information
- Always uses exactly 2 scan keys since shared objects don't have sub-object identifiers
- Uses SharedDescriptionObjIndexId for efficient indexed lookups by (objoid, classoid)
- Acquires RowExclusiveLock on pg_shdescription during both open and close operations
- Simpler than DeleteComments due to lack of sub-object complexity
- Integral part of the cascade deletion process for shared objects