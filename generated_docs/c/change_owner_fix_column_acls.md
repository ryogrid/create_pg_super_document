# change_owner_fix_column_acls

## Location
src/backend/commands/tablecmds.c: 14717 - 14781

## Overview
change_owner_fix_column_acls is a helper function for ATExecChangeOwner that scans all columns of a table and updates any non-null column-level access control lists (ACLs) to reflect the new table owner.

## Definition
```c
static void
change_owner_fix_column_acls(Oid relationOid, Oid oldOwnerId, Oid newOwnerId)
```

## Detailed Description
This function is responsible for updating column-level permissions when a table's ownership changes. It systematically scans through all columns of the specified relation in the pg_attribute system catalog, identifies columns that have explicit ACLs set (non-null attacl values), and updates those ACLs to reflect the ownership change using the aclnewowner function. The function skips dropped columns and only processes columns that actually have ACL entries, as null ACLs inherit permissions from the table level and don't require updates.

The function uses a system catalog scan to iterate through all attributes of the relation, extracts existing ACL information, transforms it for the new owner, and updates the catalog entry. This ensures that column-level permissions are properly maintained when table ownership changes.

## Parameters
- `relationOid`: OID of the relation whose column ACLs need to be updated
- `oldOwnerId`: OID of the previous owner (used by aclnewowner for proper ACL transformation)
- `newOwnerId`: OID of the new owner

## Dependencies
- Functions called/Symbols referenced:
  - table_open, table_close
  - ScanKeyInit, systable_beginscan, systable_getnext, systable_endscan
  - heap_getattr, heap_modify_tuple, heap_freetuple
  - CatalogTupleUpdate
  - aclnewowner, DatumGetAclP
  - Form_pg_attribute, SysScanDesc, Acl
- Called from:
  - ATExecChangeOwner

## Notes and Other Information
- Only processes columns with explicit ACLs (non-null attacl values)
- Skips dropped columns (attisdropped = true) as they don't need ACL updates
- Uses AttributeRelidNumIndexId for efficient scanning by relation OID
- Maintains transactional consistency by updating the catalog within the same transaction
- Essential for preserving column-level security permissions during ownership transfers
- Works in conjunction with relation-level ACL updates performed by the calling function