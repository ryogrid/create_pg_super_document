# pg_largeobject_aclcheck_snapshot

## Location
[src/backend/catalog/aclchk.c:4133-4146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4133-L4146)

## Overview
Checks a user's access privileges to a large object using a specific snapshot for consistent visibility of the access control information.

## Definition
```c
AclResult pg_largeobject_aclcheck_snapshot(Oid lobj_oid, Oid roleid, AclMode mode, Snapshot snapshot)
```

## Detailed Description
This function provides an exported routine for checking access privileges to PostgreSQL large objects with snapshot-based consistency. It acts as a wrapper around `pg_largeobject_aclmask_snapshot`, providing a simple boolean-like interface for large object access control checks. The function uses a specific snapshot to ensure consistent visibility of access control data, which is important for transactional consistency in concurrent environments.

## Parameters / Member Variables
- `lobj_oid`: The OID of the large object whose access privileges are being checked
- `roleid`: The OID of the role whose privileges are being verified
- `mode`: The type of access being requested (AclMode enumeration)
- `snapshot`: The snapshot to use for consistent visibility of access control data

## Dependencies
- Functions called/Symbols referenced:
  - [pg_largeobject_aclmask_snapshot](pg_largeobject_aclmask_snapshot.md)
  - ACLMASK_ANY
  - ACLCHECK_NO_PRIV
- Called from (representative examples):
  - [inv_open](../i/inv_open.md)

## Notes and Other Information
- Located in src/backend/catalog/aclchk.c:4133-4146
- This function is specifically used in the large object API for access control
- The snapshot parameter ensures transactional consistency when checking privileges
- Returns ACLCHECK_OK if the role has the required privileges, ACLCHECK_NO_PRIV otherwise
- Primary caller is inv_open which opens large objects for reading/writing
- Part of PostgreSQL's large object storage system access control

## Simplified Source

```c
AclResult pg_largeobject_aclcheck_snapshot(Oid lobj_oid, Oid roleid,
                                          AclMode mode, Snapshot snapshot) {
    // Check if role has the requested privileges on the large object
    if (pg_largeobject_aclmask_snapshot(lobj_oid, roleid, mode,
                                       ACLMASK_ANY, snapshot) != 0)
        return ACLCHECK_OK;
    else
        return ACLCHECK_NO_PRIV;
}
```