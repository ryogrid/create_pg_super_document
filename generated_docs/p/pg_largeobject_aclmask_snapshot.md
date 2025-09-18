# pg_largeobject_aclmask_snapshot

## Location
[src/backend/catalog/aclchk.c:3592-3664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3592-L3664)

## Overview
A specialized function that examines a user's privileges for PostgreSQL large objects using a specific MVCC snapshot to ensure consistent permission checking relative to the data access context.

## Definition


## Detailed Description
This function provides privilege checking for PostgreSQL large objects (LOBs) with special consideration for MVCC (Multi-Version Concurrency Control) consistency. The key distinguishing feature is the snapshot parameter, which ensures that permission checks are performed relative to the same snapshot that will be used to access the underlying large object data.

The function's workflow:
1. **Superuser Bypass**: Immediately grants all requested permissions to superusers
2. **Metadata Access**: Opens the pg_largeobject_metadata system table with appropriate locking
3. **Snapshot-Consistent Scanning**: Uses the provided snapshot to scan for the large object's metadata entry
4. **Existence Validation**: Verifies the large object exists, throwing an error if not found
5. **Owner Identification**: Retrieves the large object's owner from the metadata
6. **ACL Processing**: Handles both explicit ACLs and default permissions for large objects
7. **Permission Evaluation**: Uses the standard aclmask function with the object's owner and ACL

The snapshot parameter is crucial for maintaining consistency when large objects are opened for reading using the caller's snapshot, as documented in the PostgreSQL large object documentation.

## Parameters / Member Variables
- : The OID of the large object to check permissions for
- : The OID of the role whose permissions are being checked
- : Bitmask specifying which permissions to check (ACL_SELECT for reading, ACL_UPDATE for writing)
- : Specifies how to combine multiple ACL entries (ACLMASK_ALL or ACLMASK_ANY)
- : The MVCC snapshot to use for consistent metadata access (NULL for current snapshot)

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - [pfree](pfree.md)
- Called from (representative examples):
  - InternalDefaultACL
  - [pg_aclmask](pg_aclmask.md)
  - [pg_largeobject_aclcheck_snapshot](pg_largeobject_aclcheck_snapshot.md)

## Notes and Other Information
- This is a static (internal) function, not directly accessible outside aclchk.c
- Uses system table scanning rather than system cache for snapshot consistency
- Designed specifically for large object access patterns where read consistency is important
- Throws ERRCODE_UNDEFINED_OBJECT error if the large object doesn't exist
- Uses AccessShareLock when opening pg_largeobject_metadata to allow concurrent access
- The snapshot parameter enables consistent permission checking when large objects are opened relative to a specific point in time
- Proper resource management includes closing the relation and ending the scan
- Default ACL creation uses OBJECT_LARGEOBJECT type with the actual object owner
- Memory management includes cleanup of detoasted ACL data