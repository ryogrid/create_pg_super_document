# AlterIndexNamespaces

## Location
src/backend/commands/tablecmds.c: 17392 - 17436

## Overview
A static function that moves all indexes associated with a specified relation from one namespace to another as part of table namespace changes.

## Definition
```c
static void AlterIndexNamespaces(Relation classRel, Relation rel,
                                Oid oldNspOid, Oid newNspOid, ObjectAddresses *objsMoved)
```

## Detailed Description
This function handles the namespace relocation of all indexes belonging to a relation when the parent table is moved between schemas. It iterates through the relation's index list and calls AlterRelationNamespaceInternal for each index. The function includes duplicate checking to avoid processing the same index multiple times and assumes proper permission validation has been performed by the caller.

## Parameters / Member Variables
- `classRel`: Pre-opened and write-locked pg_class relation for catalog operations
- `rel`: The parent relation whose indexes are being moved
- `oldNspOid`: Object identifier of the source namespace
- `newNspOid`: Object identifier of the destination namespace
- `objsMoved`: Collection tracking objects already processed to prevent duplicates

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [object_address_present](../o/object_address_present.md)
  - [AlterRelationNamespaceInternal](AlterRelationNamespaceInternal.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)

## Notes and Other Information
- Indexes do not have their own namespace dependencies, so changeDependencyFor is not needed
- No corresponding pg_type row exists for indexes, simplifying the operation
- The objsMoved duplicate check may be redundant due to single dependency links
- Critical component of ALTER TABLE SET SCHEMA operations
- Static function scope limits its use to within tablecmds.c