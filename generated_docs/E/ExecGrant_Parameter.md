# ExecGrant_Parameter

## Location
[src/backend/catalog/aclchk.c:2472-2614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2472-L2614)

## Overview
ExecGrant_Parameter handles GRANT and REVOKE operations on PostgreSQL configuration parameters (GUCs), managing privileges stored in the pg_parameter_acl catalog.

## Definition

```c
static void
ExecGrant_Parameter(InternalGrant *istmt)
```
## Detailed Description
ExecGrant_Parameter implements privilege management for PostgreSQL configuration parameters. This function allows granting SET and ALTER SYSTEM privileges on specific GUC parameters to non-superuser roles. Unlike other objects, parameters are treated as owned by the bootstrap superuser (BOOTSTRAP_SUPERUSERID) and use a dedicated pg_parameter_acl catalog for storing ACLs.

The function includes an optimization where if the new ACL matches the default privileges, it removes the catalog entry entirely rather than storing a redundant default ACL. This keeps the parameter ACL catalog compact by only storing non-default privilege grants.

## Parameters / Member Variables
- : Internal representation of the GRANT/REVOKE statement containing parameter names/OIDs, grantees, privileges, and options

## Dependencies
- Functions called/Symbols referenced:
  - table_open, table_close (with ParameterAclRelationId)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (with PARAMETERACLOID)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md), SysCacheGetAttrNotNull
  - TextDatumGetCString (for parameter name extraction)
  - [acldefault](../a/acldefault.md), aclmembers, aclequal
  - select_best_grantor
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md) (with OBJECT_PARAMETER_ACL)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md), CatalogTupleUpdate, CatalogTupleDelete
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - CommandCounterIncrement
- Called from:
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md) (when processing parameter privileges)

## Notes and Other Information
- All parameters are treated as owned by BOOTSTRAP_SUPERUSERID regardless of who created them
- Uses PARAMETERACLOID syscache for efficient parameter ACL lookups
- Supports ACL_ALL_RIGHTS_PARAMETER_ACL privilege set (SET and ALTER SYSTEM)
- Optimizes storage by deleting catalog entries when ACL equals default privileges
- Part of PostgreSQL's security model allowing delegation of parameter management to non-superusers
- Parameter names are stored as text values and must be extracted using TextDatumGetCString