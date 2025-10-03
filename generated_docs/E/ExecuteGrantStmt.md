# ExecuteGrantStmt

## Location
[src/backend/catalog/aclchk.c:392-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L392-L601)

## Overview
Main entry point for executing GRANT and REVOKE SQL utility commands, converting the parsed statement into an internal representation and delegating to the actual execution logic.

## Definition

```c
void
ExecuteGrantStmt(GrantStmt *stmt)
```
## Detailed Description
This public function serves as the primary interface for PostgreSQL's GRANT and REVOKE command execution. It performs comprehensive validation and transformation of the parsed GrantStmt into an InternalGrant structure. The function first validates the grantor specification (currently limited to the current user for SQL compatibility). It then resolves object names to OIDs using either objectNamesToOids() for specific objects or objectsInSchemaToOids() for schema-wide operations. Role specifications are converted from RoleSpec structures to OID lists, with special handling for PUBLIC grants. The function maps privilege specifications from string names to AclMode bitmasks, validating that requested privileges are appropriate for the target object type. Column-level privileges are separated for special handling. Finally, it delegates to ExecGrantStmt_oids() for the actual ACL modifications. The function includes extensive object type handling for all PostgreSQL objects that support ACL-based security.

## Parameters / Member Variables
- `*stmt`: Pointer to the parsed GrantStmt structure containing all GRANT/REVOKE statement components including target objects, grantees, privileges, and options
## Dependencies
- Functions called/Symbols referenced:
  - [get_rolespec_oid](../g/get_rolespec_oid.md)
  - [objectNamesToOids](../o/objectNamesToOids.md)
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md)  
  - [lappend_oid](../l/lappend_oid.md)
  - [string_to_privilege](../s/string_to_privilege.md)
  - [privilege_to_string](../p/privilege_to_string.md)
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md)
  - ereport
  - elog
  - gettext_noop
- Types and structures:
  - [GrantStmt](../G/GrantStmt.md)
  - [InternalGrant](../I/InternalGrant.md)
  - [RoleSpec](../R/RoleSpec.md)
  - [AccessPriv](../A/AccessPriv.md)
  - AclMode
- Constants used:
  - All OBJECT_* type constants
  - All ACL_ALL_RIGHTS_* constants
  - ACL_TARGET_OBJECT, ACL_TARGET_ALL_IN_SCHEMA
  - ACL_ID_PUBLIC
  - ROLESPEC_PUBLIC
- Called from:
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Currently enforces that the grantor must be the current user for SQL standard compliance, though this restriction may be relaxed in future versions
- Handles both individual object grants and schema-wide grants (ALL IN SCHEMA syntax)
- Special logic for table objects that might actually be sequences, requiring validation of both relation and sequence privilege types
- Column-level privileges are only valid for table objects and are handled separately from table-level privileges  
- Supports all PostgreSQL object types: tables, sequences, databases, functions, schemas, types, tablespaces, foreign data wrappers, foreign servers, and configuration parameters
- The function performs privilege name validation, ensuring requested privileges are valid for the target object type
- Acts as a facade that transforms external SQL syntax into internal representation before delegating to the core execution logic

## Simplified Source

```c
void ExecuteGrantStmt(GrantStmt *stmt)
{
    InternalGrant istmt;
    ListCell *cell;
    const char *errormsg;
    AclMode all_privileges;

    // Validate grantor (currently must be current user)
    if (stmt->grantor)
    {
        Oid grantor = get_rolespec_oid(stmt->grantor, false);
        if (grantor != GetUserId())
            ereport(ERROR, /* grantor must be current user */);
    }

    // Initialize internal grant structure
    istmt.is_grant = stmt->is_grant;
    istmt.objtype = stmt->objtype;

    // Convert target objects to OIDs
    switch (stmt->targtype)
    {
        case ACL_TARGET_OBJECT:
            istmt.objects = objectNamesToOids(stmt->objtype, stmt->objects, stmt->is_grant);
            break;
        case ACL_TARGET_ALL_IN_SCHEMA:
            istmt.objects = objectsInSchemaToOids(stmt->objtype, stmt->objects);
            break;
        default:
            elog(ERROR, "unrecognized GrantStmt.targtype: %d", (int) stmt->targtype);
    }

    // Initialize other fields
    istmt.col_privs = NIL;
    istmt.grantees = NIL;
    istmt.grant_option = stmt->grant_option;
    istmt.behavior = stmt->behavior;

    // Convert grantee role specifications to OIDs
    foreach(cell, stmt->grantees)
    {
        RoleSpec *grantee = (RoleSpec *) lfirst(cell);
        Oid grantee_uid;

        if (grantee->roletype == ROLESPEC_PUBLIC)
            grantee_uid = ACL_ID_PUBLIC;
        else
            grantee_uid = get_rolespec_oid(grantee, false);

        istmt.grantees = lappend_oid(istmt.grantees, grantee_uid);
    }

    // Determine appropriate privilege set for object type
    switch (stmt->objtype)
    {
        case OBJECT_TABLE:
            all_privileges = ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE;
            errormsg = gettext_noop("invalid privilege type %s for relation");
            break;
        case OBJECT_SEQUENCE:
            all_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            errormsg = gettext_noop("invalid privilege type %s for sequence");
            break;
        case OBJECT_DATABASE:
            all_privileges = ACL_ALL_RIGHTS_DATABASE;
            errormsg = gettext_noop("invalid privilege type %s for database");
            break;
        case OBJECT_FUNCTION:
        case OBJECT_PROCEDURE:
        case OBJECT_ROUTINE:
            all_privileges = ACL_ALL_RIGHTS_FUNCTION;
            errormsg = gettext_noop("invalid privilege type %s for function");
            break;
        case OBJECT_SCHEMA:
            all_privileges = ACL_ALL_RIGHTS_SCHEMA;
            errormsg = gettext_noop("invalid privilege type %s for schema");
            break;
        // ... other object types ...
        default:
            elog(ERROR, "unrecognized GrantStmt.objtype: %d", (int) stmt->objtype);
    }

    // Convert privilege specifications to AclMode bitmask
    if (stmt->privileges == NIL)
    {
        // Grant ALL privileges
        istmt.all_privs = true;
        istmt.privileges = ACL_NO_RIGHTS;
    }
    else
    {
        // Convert specific privilege names to bitmask
        istmt.all_privs = false;
        istmt.privileges = ACL_NO_RIGHTS;

        foreach(cell, stmt->privileges)
        {
            AccessPriv *privnode = (AccessPriv *) lfirst(cell);
            AclMode priv;

            // Handle column-level privileges separately
            if (privnode->cols)
            {
                if (stmt->objtype != OBJECT_TABLE)
                    ereport(ERROR, /* column privileges only valid for relations */);
                istmt.col_privs = lappend(istmt.col_privs, privnode);
                continue;
            }

            // Convert privilege name to bitmask
            priv = string_to_privilege(privnode->priv_name);

            // Validate privilege is valid for this object type
            if (priv & ~((AclMode) all_privileges))
                ereport(ERROR, /* invalid privilege type for object */);

            istmt.privileges |= priv;
        }
    }

    // Execute the grant/revoke operation
    ExecGrantStmt_oids(&istmt);
}
```