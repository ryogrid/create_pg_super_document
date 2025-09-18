# GetComment

## Location
[src/backend/commands/comment.c:410-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L410-L459)

## Overview
Retrieves the comment text associated with a specific database object from the pg_description system catalog, or returns null if no comment is found.

## Definition
```c
char *GetComment(Oid oid, Oid classoid, int32 subid)
```

## Detailed Description
The GetComment function performs a lookup in the pg_description system catalog to find and return the comment text for a specified database object. It uses a three-key search based on the object OID, class OID, and sub-object ID to uniquely identify the target object. The function opens the pg_description relation with AccessShareLock, performs an indexed scan using the DescriptionObjIndexId index, and retrieves the description field from the matching tuple. If a comment is found, it converts the text datum to a C string and returns it; otherwise, it returns NULL.

## Parameters / Member Variables
- `oid`: The object identifier (OID) of the database object whose comment is being retrieved
- `classoid`: The class OID that identifies the type of object (e.g., table, function, etc.)
- `subid`: Sub-object identifier used to distinguish parts of composite objects (e.g., column number for table columns, 0 for the object itself)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - table_open
  - RelationGetDescr
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_getattr](../h/heap_getattr.md)
  - TextDatumGetCString
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
- Called from (representative examples):
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)
  - [RebuildConstraintComment](../R/RebuildConstraintComment.md)
  - [transformTableLikeClause](../t/transformTableLikeClause.md)
  - [expandTableLikeClause](../e/expandTableLikeClause.md)

## Notes and Other Information
- The function assumes there can be only one matching comment per object and breaks after finding the first match
- Uses AccessShareLock to ensure read consistency while allowing concurrent access
- Returns a palloc()ed string that must be freed by the caller if not NULL
- The three-key lookup (oid, classoid, subid) provides precise object identification within PostgreSQL's object hierarchy
- Commonly used during DDL operations like ALTER TYPE and CREATE TABLE LIKE to preserve or copy object comments