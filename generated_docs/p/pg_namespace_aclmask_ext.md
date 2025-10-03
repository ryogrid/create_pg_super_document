# pg_namespace_aclmask_ext

## Location
[src/backend/catalog/aclchk.c:3665-3766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3665-L3766)

## Overview
This is an internal function that examines a user's privileges for a namespace (schema), with support for handling missing objects gracefully through an optional is_missing parameter.

## Definition

```c
static AclMode
pg_namespace_aclmask_ext(Oid nsp_oid, Oid roleid,
						 AclMode mask, AclMaskHow how,
						 bool *is_missing)
```
## Detailed Description
The function performs comprehensive privilege checking for PostgreSQL namespaces (schemas). It handles several special cases including superuser bypass, temporary namespace permissions, and role-based access for pg_read_all_data/pg_write_all_data roles. The function retrieves the Access Control List (ACL) from pg_namespace system catalog and evaluates permissions against it. If the is_missing parameter is provided, the function can return gracefully when the namespace doesn't exist rather than throwing an error.

## Parameters / Member Variables
- `nsp_oid`: The Object ID of the namespace to check permissions for
- `roleid`: The Object ID of the role whose permissions are being checked
- `mask`: The permission mask specifying which privileges to check (e.g., ACL_USAGE, ACL_CREATE)
- `how`: Enumeration specifying how to combine privileges (AclMaskHow type)
- `*is_missing`: Optional pointer to bool that gets set to true if the namespace doesn't exist (allows graceful handling)
## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - [isTempNamespace](../i/isTempNamespace.md)  
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
- Called from (representative examples):
  - InternalDefaultACL
  - [object_aclmask_ext](../o/object_aclmask_ext.md)

## Notes and Other Information
- Superusers automatically bypass all permission checks
- Special handling for temporary namespaces: grants all standard rights if user has CREATE TEMP on database, otherwise only USAGE
- For missing namespaces, can either return 0 permissions (if is_missing provided) or throw ERRCODE_UNDEFINED_SCHEMA error
- Automatically grants ACL_USAGE to members of pg_read_all_data or pg_write_all_data roles if not already granted
- Function is static (internal to aclchk.c) and used primarily by the broader object permission checking infrastructure

## Simplified Source

```c
static AclMode pg_namespace_aclmask_ext(Oid nsp_oid, Oid roleid,
                                       AclMode mask, AclMaskHow how,
                                       bool *is_missing)
{
    AclMode result;
    HeapTuple tuple;
    Datum aclDatum;
    bool isNull;
    Acl *acl;
    Oid ownerId;

    // Superusers bypass all permission checking
    if (superuser_arg(roleid))
        return mask;

    // Special handling for temp namespaces
    if (isTempNamespace(nsp_oid)) {
        if (object_aclcheck_ext(DatabaseRelationId, MyDatabaseId, roleid,
                               ACL_CREATE_TEMP, is_missing) == ACLCHECK_OK)
            return mask & ACL_ALL_RIGHTS_SCHEMA;
        else
            return mask & ACL_USAGE;
    }

    // Get the schema's ACL from pg_namespace
    tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
    if (!HeapTupleIsValid(tuple)) {
        if (is_missing != NULL) {
            *is_missing = true;
            return 0;
        }
        else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                           errmsg("schema with OID %u does not exist", nsp_oid)));
    }

    ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;

    aclDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspacl, &isNull);
    if (isNull) {
        // No ACL, so build default ACL
        acl = acldefault(OBJECT_SCHEMA, ownerId);
        aclDatum = (Datum) 0;
    }
    else {
        // detoast ACL if necessary
        acl = DatumGetAclP(aclDatum);
    }

    result = aclmask(acl, roleid, ownerId, mask, how);

    // Clean up detoasted copy
    if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
        pfree(acl);

    ReleaseSysCache(tuple);

    // Check for pg_read_all_data/pg_write_all_data role privileges
    if (mask & ACL_USAGE && !(result & ACL_USAGE) &&
        (has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA) ||
         has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA)))
        result |= ACL_USAGE;

    return result;
}
```