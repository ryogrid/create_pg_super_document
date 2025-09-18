# DeleteComments

## Location
src/backend/commands/comment.c: 326 - 373

## Overview
Removes comments from the pg_description catalog table for specified database objects, with support for deleting specific sub-object comments or all comments for an entire object.

## Definition


## Detailed Description
DeleteComments removes comment entries from the pg_description catalog table based on the provided object identifiers. It supports two deletion modes: when subid is nonzero, it deletes only comments for that specific sub-object (e.g., a specific column); when subid is zero, it deletes all comments associated with the object regardless of sub-object ID (used when dropping entire objects).

The function performs a systematic scan using the DescriptionObjIndexId and deletes all matching tuples. It uses variable-length scan keys depending on whether sub-object specificity is required.

## Parameters / Member Variables
- : Object identifier of the target database object whose comments should be deleted
- : OID of the system catalog containing the object (e.g., RelationRelationId for tables)
- : Sub-object identifier - if nonzero, deletes only comments for this specific sub-object; if zero, deletes all comments for the object

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the pg_description relation for modification
  - systable_beginscan: Initiates indexed scan for comments to delete
  - systable_getnext: Iterates through matching comment tuples
  - CatalogTupleDelete: Removes each matching comment tuple
  - systable_endscan: Ends the systematic scan
  - table_close: Closes the pg_description relation
- Called from (representative examples):
  - deleteOneObject: Dependency system calls during object deletion

## Notes and Other Information
- Uses conditional scan key construction: 2 keys for object-wide deletion, 3 keys for sub-object deletion
- Efficiently uses DescriptionObjIndexId for indexed lookups by (objoid, classoid, objsubid)
- Acquires RowExclusiveLock on pg_description during both open and close operations
- No memory management complexity since it only deletes existing tuples
- Typically invoked by the dependency tracking system during CASCADE deletions