# RemoveRoleFromObjectACL

## Location
[src/backend/catalog/aclchk.c:1466-1600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1466-L1600)

## Overview
Removes all mentions of a role from an object's Access Control List (ACL), used when dropping a role to clean up all associated permissions.

## Definition
```c
void RemoveRoleFromObjectACL(Oid roleid, Oid classid, Oid objid)
```

## Detailed Description
This function is used by `shdepDropOwned` to remove mentions of a role in ACLs when a role is being dropped from the system. It handles two main cases:

1. **Default ACLs** (when classid == DefaultAclRelationId): The function retrieves the default ACL information from pg_default_acl, constructs an InternalDefaultACL structure, and calls SetDefaultACL to revoke all privileges.

2. **Regular Object ACLs**: For other object types, it maps the object class ID to the appropriate object type, constructs an InternalGrant structure, and calls ExecGrantStmt_oids to perform a REVOKE ALL operation.

The function effectively performs a "REVOKE ALL" operation on the specified object for the given role. For table objects, this also revokes any column-level permissions per SQL standard behavior.

## Parameters
- `roleid`: The OID of the role to be removed from the ACL
- `classid`: The system catalog relation OID that defines the object type (e.g., RelationRelationId for tables)
- `objid`: The OID of the specific object whose ACL should be modified

## Dependencies
- Functions called/Symbols referenced:
  - [SetDefaultACL](../S/SetDefaultACL.md)
  - [ExecGrantStmt_oids](../E/ExecGrantStmt_oids.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - list_make1_oid
- Called from (representative examples):
  - [shdepDropOwned](../s/shdepDropOwned.md)

## Notes and Other Information
- The function does not accept an objsubid parameter, which means it operates at the object level rather than sub-object level (like specific columns)
- For table objects with column-level permissions, the function issues REVOKE ALL ON TABLE which also revokes column permissions according to SQL specification
- This is designed for role deletion scenarios where all permissions must be removed
- The function handles various PostgreSQL object types including tables, databases, types, procedures, languages, large objects, schemas, tablespaces, foreign servers, foreign data wrappers, and parameter ACLs

## Simplified Source
```c
void
RemoveRoleFromObjectACL(Oid roleid, Oid classid, Oid objid)
{
    if (classid == DefaultAclRelationId)
    {
        // Handle default ACL removal
        InternalDefaultACL iacls;
        Relation rel;
        ScanKeyData skey[1];
        SysScanDesc scan;
        HeapTuple tuple;

        // Fetch default ACL info from pg_default_acl
        rel = table_open(DefaultAclRelationId, AccessShareLock);
        ScanKeyInit(&skey[0], Anum_pg_default_acl_oid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
        scan = systable_beginscan(rel, DefaultAclOidIndexId, true, NULL, 1, skey);
        tuple = systable_getnext(scan);

        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "could not find tuple for default ACL %u", objid);

        Form_pg_default_acl pg_default_acl_tuple = (Form_pg_default_acl) GETSTRUCT(tuple);

        // Set up internal ACL structure
        iacls.roleid = pg_default_acl_tuple->defaclrole;
        iacls.nspid = pg_default_acl_tuple->defaclnamespace;

        // Map default ACL object type to internal object type
        switch (pg_default_acl_tuple->defaclobjtype)
        {
            case DEFACLOBJ_RELATION:  iacls.objtype = OBJECT_TABLE; break;
            case DEFACLOBJ_SEQUENCE:  iacls.objtype = OBJECT_SEQUENCE; break;
            case DEFACLOBJ_FUNCTION:  iacls.objtype = OBJECT_FUNCTION; break;
            case DEFACLOBJ_TYPE:      iacls.objtype = OBJECT_TYPE; break;
            case DEFACLOBJ_NAMESPACE: iacls.objtype = OBJECT_SCHEMA; break;
            default:
                elog(ERROR, "unexpected default ACL type: %d", (int) pg_default_acl_tuple->defaclobjtype);
        }

        systable_endscan(scan);
        table_close(rel, AccessShareLock);

        // Configure for revoke operation
        iacls.is_grant = false;
        iacls.all_privs = true;
        iacls.privileges = ACL_NO_RIGHTS;
        iacls.grantees = list_make1_oid(roleid);
        iacls.grant_option = false;
        iacls.behavior = DROP_CASCADE;

        SetDefaultACL(&iacls);
    }
    else
    {
        // Handle regular object ACL removal
        InternalGrant istmt;

        // Map class ID to object type
        switch (classid)
        {
            case RelationRelationId:        istmt.objtype = OBJECT_TABLE; break;
            case DatabaseRelationId:        istmt.objtype = OBJECT_DATABASE; break;
            case TypeRelationId:            istmt.objtype = OBJECT_TYPE; break;
            case ProcedureRelationId:       istmt.objtype = OBJECT_ROUTINE; break;
            case LanguageRelationId:        istmt.objtype = OBJECT_LANGUAGE; break;
            case LargeObjectRelationId:     istmt.objtype = OBJECT_LARGEOBJECT; break;
            case NamespaceRelationId:       istmt.objtype = OBJECT_SCHEMA; break;
            case TableSpaceRelationId:      istmt.objtype = OBJECT_TABLESPACE; break;
            case ForeignServerRelationId:   istmt.objtype = OBJECT_FOREIGN_SERVER; break;
            case ForeignDataWrapperRelationId: istmt.objtype = OBJECT_FDW; break;
            case ParameterAclRelationId:    istmt.objtype = OBJECT_PARAMETER_ACL; break;
            default:
                elog(ERROR, "unexpected object class %u", classid);
        }

        // Configure for revoke operation
        istmt.is_grant = false;
        istmt.objects = list_make1_oid(objid);
        istmt.all_privs = true;
        istmt.privileges = ACL_NO_RIGHTS;
        istmt.col_privs = NIL;
        istmt.grantees = list_make1_oid(roleid);
        istmt.grant_option = false;
        istmt.behavior = DROP_CASCADE;

        ExecGrantStmt_oids(&istmt);
    }
}
```