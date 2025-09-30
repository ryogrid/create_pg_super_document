# ExecGrantStmt_oids

## Location
[src/backend/catalog/aclchk.c:602-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L602-L668)

## Overview
Internal dispatcher function that routes privilege grant/revoke operations to the appropriate object-type-specific execution functions based on the object type.

## Definition

```c
static void
ExecGrantStmt_oids(InternalGrant *istmt)
```
## Detailed Description
This static function serves as a central dispatch mechanism for PostgreSQL's internal privilege management system. After the initial parsing and validation has been completed by ExecuteGrantStmt(), this function routes the privilege operation to the appropriate specialized handler based on the object type. It uses a comprehensive switch statement to map each object type to its corresponding execution function - tables and sequences use ExecGrant_Relation(), large objects use ExecGrant_Largeobject(), parameters use ExecGrant_Parameter(), while most other object types use the generic ExecGrant_common() with object-specific parameters including catalog relation ID, privilege mask, and optional validation callback functions. After the privilege changes are successfully applied, the function triggers event notification through the event trigger system if the object type supports it, allowing extensions and logging systems to capture privilege change events.

## Parameters / Member Variables
- : Pointer to InternalGrant structure containing all processed privilege operation details including object OIDs, grantee OIDs, privilege specifications, and operation flags

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGrant_Relation](ExecGrant_Relation.md)
  - [ExecGrant_common](ExecGrant_common.md)
  - [ExecGrant_Largeobject](ExecGrant_Largeobject.md)
  - [ExecGrant_Parameter](ExecGrant_Parameter.md)
  - [EventTriggerSupportsObjectType](EventTriggerSupportsObjectType.md)
  - [EventTriggerCollectGrant](EventTriggerCollectGrant.md)
  - elog
- Validation callbacks used:
  - [ExecGrant_Type_check](ExecGrant_Type_check.md)
  - [ExecGrant_Language_check](ExecGrant_Language_check.md)
- Catalog relation constants:
  - DatabaseRelationId
  - TypeRelationId
  - ForeignDataWrapperRelationId
  - ForeignServerRelationId
  - ProcedureRelationId
  - LanguageRelationId
  - NamespaceRelationId
  - TableSpaceRelationId
- Object types and privilege constants:
  - All OBJECT_* type constants
  - All ACL_ALL_RIGHTS_* privilege masks
- Called from:
  - [ExecuteGrantStmt](ExecuteGrantStmt.md)
  - InternalDefaultACL
  - [RemoveRoleFromObjectACL](../R/RemoveRoleFromObjectACL.md)

## Notes and Other Information
- This function represents the boundary between generic privilege processing and object-type-specific implementation details
- Tables and sequences are grouped together because they share similar privilege semantics and catalog structure
- Functions, procedures, and routines are all handled by the same execution path since they all use the pg_proc catalog
- Domains and types are grouped together since they both use the pg_type catalog and have identical privilege semantics
- Some object types require additional validation through callback functions (types and languages) while others do not
- Event trigger notification occurs after successful privilege modification to ensure triggers see the actual changes made
- The function maintains PostgreSQL's extensibility by supporting event triggers for privilege operations on supported object types
- Error handling ensures that unsupported object types are caught and reported rather than silently ignored

## Simplified Source

```c
static void ExecGrantStmt_oids(InternalGrant *istmt) {
    // Route to appropriate grant handler based on object type
    switch (istmt->objtype) {
        // Tables and sequences use relation-specific handler
        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
            ExecGrant_Relation(istmt);
            break;

        // Most object types use common handler with type-specific parameters
        case OBJECT_DATABASE:
            ExecGrant_common(istmt, DatabaseRelationId, ACL_ALL_RIGHTS_DATABASE, NULL);
            break;
        case OBJECT_DOMAIN:
        case OBJECT_TYPE:
            ExecGrant_common(istmt, TypeRelationId, ACL_ALL_RIGHTS_TYPE, ExecGrant_Type_check);
            break;
        case OBJECT_FUNCTION:
        case OBJECT_PROCEDURE:
        case OBJECT_ROUTINE:
            ExecGrant_common(istmt, ProcedureRelationId, ACL_ALL_RIGHTS_FUNCTION, NULL);
            break;
        case OBJECT_LANGUAGE:
            ExecGrant_common(istmt, LanguageRelationId, ACL_ALL_RIGHTS_LANGUAGE, ExecGrant_Language_check);
            break;
        case OBJECT_SCHEMA:
            ExecGrant_common(istmt, NamespaceRelationId, ACL_ALL_RIGHTS_SCHEMA, NULL);
            break;
        case OBJECT_TABLESPACE:
            ExecGrant_common(istmt, TableSpaceRelationId, ACL_ALL_RIGHTS_TABLESPACE, NULL);
            break;
        case OBJECT_FDW:
            ExecGrant_common(istmt, ForeignDataWrapperRelationId, ACL_ALL_RIGHTS_FDW, NULL);
            break;
        case OBJECT_FOREIGN_SERVER:
            ExecGrant_common(istmt, ForeignServerRelationId, ACL_ALL_RIGHTS_FOREIGN_SERVER, NULL);
            break;

        // Special object types with dedicated handlers
        case OBJECT_LARGEOBJECT:
            ExecGrant_Largeobject(istmt);
            break;
        case OBJECT_PARAMETER_ACL:
            ExecGrant_Parameter(istmt);
            break;

        default:
            elog(ERROR, "unrecognized GrantStmt.objtype: %d", (int) istmt->objtype);
    }

    // Notify event triggers about completed grant operation
    if (EventTriggerSupportsObjectType(istmt->objtype)) {
        EventTriggerCollectGrant(istmt);
    }
}
```