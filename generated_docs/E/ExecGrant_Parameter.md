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
  - [table_open](../t/table_open.md), table_close (with ParameterAclRelationId)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (with PARAMETERACLOID)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md), SysCacheGetAttrNotNull
  - TextDatumGetCString (for parameter name extraction)
  - [acldefault](../a/acldefault.md), aclmembers, aclequal
  - [select_best_grantor](../s/select_best_grantor.md)
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md) (with OBJECT_PARAMETER_ACL)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md), CatalogTupleUpdate, CatalogTupleDelete
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from:
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md) (when processing parameter privileges)

## Notes and Other Information
- All parameters are treated as owned by BOOTSTRAP_SUPERUSERID regardless of who created them
- Uses PARAMETERACLOID syscache for efficient parameter ACL lookups
- Supports ACL_ALL_RIGHTS_PARAMETER_ACL privilege set (SET and ALTER SYSTEM)
- Optimizes storage by deleting catalog entries when ACL equals default privileges
- Part of PostgreSQL's security model allowing delegation of parameter management to non-superusers
- Parameter names are stored as text values and must be extracted using TextDatumGetCString

## Simplified Source

```c
static void
ExecGrant_Parameter(InternalGrant *istmt)
{
    Relation relation;
    ListCell *cell;

    // Set default privileges if ALL PRIVILEGES specified
    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
        istmt->privileges = ACL_ALL_RIGHTS_PARAMETER_ACL;

    // Open parameter ACL catalog
    relation = table_open(ParameterAclRelationId, RowExclusiveLock);

    // Process each parameter
    foreach(cell, istmt->objects)
    {
        Oid parameterId = lfirst_oid(cell);
        Datum nameDatum, aclDatum;
        const char *parname;
        bool isNull;
        AclMode avail_goptions, this_privileges;
        Acl *old_acl, *new_acl;
        Oid grantorId, ownerId;
        HeapTuple tuple;
        // ... variable declarations ...

        // Find parameter ACL tuple
        tuple = SearchSysCache1(PARAMETERACLOID, ObjectIdGetDatum(parameterId));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for parameter ACL %u", parameterId);

        // Extract parameter name
        nameDatum = SysCacheGetAttrNotNull(PARAMETERACLOID, tuple,
                                           Anum_pg_parameter_acl_parname);
        parname = TextDatumGetCString(nameDatum);

        // All parameters owned by bootstrap superuser
        ownerId = BOOTSTRAP_SUPERUSERID;

        // Get existing ACL or use default
        aclDatum = SysCacheGetAttr(PARAMETERACLOID, tuple,
                                   Anum_pg_parameter_acl_paracl, &isNull);
        if (isNull)
            old_acl = acldefault(istmt->objtype, ownerId);
        else
            old_acl = DatumGetAclPCopy(aclDatum);

        // Determine grantor and validate privileges
        select_best_grantor(GetUserId(), istmt->privileges, old_acl, ownerId,
                            &grantorId, &avail_goptions);
        this_privileges = restrict_and_check_grant(istmt->is_grant, avail_goptions,
                                                   istmt->all_privs, istmt->privileges,
                                                   parameterId, grantorId,
                                                   OBJECT_PARAMETER_ACL, parname, 0, NULL);

        // Generate new ACL
        new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
                                       istmt->grant_option, istmt->behavior,
                                       istmt->grantees, this_privileges,
                                       grantorId, ownerId);

        // Update or delete catalog entry based on whether ACL equals default
        if (aclequal(new_acl, acldefault(istmt->objtype, ownerId)))
        {
            // Remove entry if ACL equals default
            CatalogTupleDelete(relation, &tuple->t_self);
        }
        else
        {
            // Update with new ACL
            HeapTuple newtuple;
            replaces[Anum_pg_parameter_acl_paracl - 1] = true;
            values[Anum_pg_parameter_acl_paracl - 1] = PointerGetDatum(new_acl);
            newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation),
                                         values, nulls, replaces);
            CatalogTupleUpdate(relation, &newtuple->t_self, newtuple);
        }

        // Update dependencies and extension privileges
        recordExtensionInitPriv(parameterId, ParameterAclRelationId, 0, new_acl);
        updateAclDependencies(ParameterAclRelationId, parameterId, 0, ownerId,
                              noldmembers, oldmembers, nnewmembers, newmembers);

        ReleaseSysCache(tuple);
        pfree(new_acl);
        CommandCounterIncrement();
    }

    table_close(relation, RowExclusiveLock);
}
```