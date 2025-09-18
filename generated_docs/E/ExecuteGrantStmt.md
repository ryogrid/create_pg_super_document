# ExecuteGrantStmt

## Location
[src/backend/catalog/aclchk.c:392-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L392-L601)

## Overview
Main entry point for executing GRANT and REVOKE SQL utility commands, converting the parsed statement into an internal representation and delegating to the actual execution logic.

## Definition


## Detailed Description
This public function serves as the primary interface for PostgreSQL's GRANT and REVOKE command execution. It performs comprehensive validation and transformation of the parsed GrantStmt into an InternalGrant structure. The function first validates the grantor specification (currently limited to the current user for SQL compatibility). It then resolves object names to OIDs using either objectNamesToOids() for specific objects or objectsInSchemaToOids() for schema-wide operations. Role specifications are converted from RoleSpec structures to OID lists, with special handling for PUBLIC grants. The function maps privilege specifications from string names to AclMode bitmasks, validating that requested privileges are appropriate for the target object type. Column-level privileges are separated for special handling. Finally, it delegates to ExecGrantStmt_oids() for the actual ACL modifications. The function includes extensive object type handling for all PostgreSQL objects that support ACL-based security.

## Parameters / Member Variables
- : Pointer to the parsed GrantStmt structure containing all GRANT/REVOKE statement components including target objects, grantees, privileges, and options

## Dependencies
- Functions called/Symbols referenced:
  - get_rolespec_oid
  - [objectNamesToOids](../o/objectNamesToOids.md)
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md)  
  - lappend_oid
  - [string_to_privilege](../s/string_to_privilege.md)
  - [privilege_to_string](../p/privilege_to_string.md)
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md)
  - ereport
  - elog
  - gettext_noop
- Types and structures:
  - GrantStmt
  - InternalGrant
  - [RoleSpec](../R/RoleSpec.md)
  - AccessPriv
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