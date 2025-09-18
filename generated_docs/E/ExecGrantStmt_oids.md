# ExecGrantStmt_oids

## Location
src/backend/catalog/aclchk.c: 602 - 668

## Overview
Internal dispatcher function that routes privilege grant/revoke operations to the appropriate object-type-specific execution functions based on the object type.

## Definition


## Detailed Description
This static function serves as a central dispatch mechanism for PostgreSQL's internal privilege management system. After the initial parsing and validation has been completed by ExecuteGrantStmt(), this function routes the privilege operation to the appropriate specialized handler based on the object type. It uses a comprehensive switch statement to map each object type to its corresponding execution function - tables and sequences use ExecGrant_Relation(), large objects use ExecGrant_Largeobject(), parameters use ExecGrant_Parameter(), while most other object types use the generic ExecGrant_common() with object-specific parameters including catalog relation ID, privilege mask, and optional validation callback functions. After the privilege changes are successfully applied, the function triggers event notification through the event trigger system if the object type supports it, allowing extensions and logging systems to capture privilege change events.

## Parameters / Member Variables
- : Pointer to InternalGrant structure containing all processed privilege operation details including object OIDs, grantee OIDs, privilege specifications, and operation flags

## Dependencies
- Functions called/Symbols referenced:
  - ExecGrant_Relation
  - ExecGrant_common
  - ExecGrant_Largeobject
  - ExecGrant_Parameter
  - EventTriggerSupportsObjectType
  - EventTriggerCollectGrant
  - elog
- Validation callbacks used:
  - ExecGrant_Type_check
  - ExecGrant_Language_check
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
  - ExecuteGrantStmt
  - InternalDefaultACL
  - RemoveRoleFromObjectACL

## Notes and Other Information
- This function represents the boundary between generic privilege processing and object-type-specific implementation details
- Tables and sequences are grouped together because they share similar privilege semantics and catalog structure
- Functions, procedures, and routines are all handled by the same execution path since they all use the pg_proc catalog
- Domains and types are grouped together since they both use the pg_type catalog and have identical privilege semantics
- Some object types require additional validation through callback functions (types and languages) while others do not
- Event trigger notification occurs after successful privilege modification to ensure triggers see the actual changes made
- The function maintains PostgreSQL's extensibility by supporting event triggers for privilege operations on supported object types
- Error handling ensures that unsupported object types are caught and reported rather than silently ignored