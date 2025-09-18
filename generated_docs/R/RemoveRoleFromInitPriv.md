# RemoveRoleFromInitPriv

## Location
src/backend/catalog/aclchk.c: 4922 - 5049

## Overview
A function used by shdepDropOwned to remove mentions of a role from pg_init_privs entries when a role is being dropped.

## Definition
void RemoveRoleFromInitPriv(Oid roleid, Oid classid, Oid objid, int32 objsubid)

## Detailed Description
This function is designed to support role dropping operations by removing all references to a specific role from initial privilege records stored in pg_init_privs. When a role is being dropped, this function locates the corresponding initial privilege entry and removes all privileges granted to or by the specified role. The function uses merge_acl_with_grant with is_grant=false to effectively revoke all privileges associated with the role.

The function requires determining the object's owner to properly process the ACL modifications, using the system cache to look up owner information. If removing the role results in an empty ACL, the entire pg_init_privs entry is deleted. The function maintains dependency tracking by updating pg_shdepend records to reflect the role removal.

## Parameters / Member Variables
- roleid: The OID of the role to be removed from the ACL
- classid: The OID of the system catalog table that defines the object type  
- objid: The OID of the object whose initial privileges are being updated
- objsubid: Sub-object identifier (0 for objects without sub-components)

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close
  - ScanKeyInit
  - systable_beginscan/systable_endscan/systable_getnext
  - heap_getattr
  - DatumGetAclPCopy
  - aclmembers
  - get_object_catcache_oid
  - SearchSysCache1/ReleaseSysCache
  - get_object_class_descr
  - SysCacheGetAttrNotNull
  - DatumGetObjectId
  - get_object_attnum_owner
  - merge_acl_with_grant
  - DROP_RESTRICT
  - list_make1_oid
  - ACLITEM_ALL_PRIV_BITS
  - ACL_NUM
  - CatalogTupleDelete
  - heap_modify_tuple
  - CatalogTupleUpdate
  - updateInitAclDependencies
  - CommandCounterIncrement
- Called from (representative examples):
  - shdepDropOwned

## Notes and Other Information
- Specifically designed for role dropping operations via shdepDropOwned
- Must determine object owner through system cache lookups to properly process ACLs
- Uses merge_acl_with_grant in revoke mode (is_grant=false) to remove role privileges
- Gracefully handles cases where no pg_init_privs entry exists for the object
- Deletes the entire entry if role removal results in an empty ACL
- Uses DROP_RESTRICT mode and ACLITEM_ALL_PRIV_BITS to remove all privileges
- Maintains consistency with pg_shdepend through updateInitAclDependencies
- Requires RowExclusiveLock on pg_init_privs to ensure update consistency
- Uses CommandCounterIncrement to handle multiple object processing