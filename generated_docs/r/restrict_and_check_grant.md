# restrict_and_check_grant

## Location
src/backend/catalog/aclchk.c: 241 - 391

## Overview
Validates and restricts privilege operations to what the grantor can actually grant or revoke, issuing appropriate SQL standard-mandated warnings and errors.

## Definition


## Detailed Description
This static function performs comprehensive privilege validation and restriction for PostgreSQL's GRANT and REVOKE operations. It first determines the complete privilege mask available for the given object type through a large switch statement covering all supported object types (tables, sequences, databases, functions, etc.). The function then validates that the grantor has sufficient privileges on the object by checking if they have any privileges at all - if not, it raises an access denied error. Next, it restricts the requested privileges to only those the grantor can actually grant (intersection of requested privileges with available grant options). Finally, it issues SQL standard-compliant warnings when no privileges were granted/revoked or when only a subset of requested privileges could be processed. The function handles special cases for column-level privileges and event triggers (which don't support grantable rights).

## Parameters / Member Variables
- : Boolean indicating whether this is a GRANT (true) or REVOKE (false) operation
- : AclMode bitmask of grant options available to the grantor for this object
- : Boolean indicating if ALL PRIVILEGES was specified (affects warning behavior)
- : AclMode bitmask of privileges being requested for grant or revoke
- : OID of the database object being operated on
- : OID of the user/role attempting to grant or revoke privileges
- : ObjectType enum specifying the type of object (table, function, etc.)
- : Human-readable name of the object for error messages
- : Attribute number for column-level operations (InvalidAttrNumber otherwise)
- : Column name for column-level operations (NULL otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - pg_aclmask
  - aclcheck_error
  - aclcheck_error_col
  - ereport
  - elog
- Macros used:
  - ACL_GRANT_OPTION_FOR
  - ACL_OPTION_TO_PRIVS
- Types and constants:
  - ObjectType and all OBJECT_* constants
  - AclMode and all ACL_ALL_RIGHTS_* constants
  - ACL_NO_RIGHTS
  - ACLMASK_ANY
  - ACLCHECK_NO_PRIV
- Called from:
  - InternalDefaultACL
  - ExecGrant_Attribute
  - ExecGrant_Relation
  - ExecGrant_common
  - ExecGrant_Largeobject
  - ExecGrant_Parameter

## Notes and Other Information
- Implements SQL standard compliance for privilege warnings - warns when no privileges are granted/revoked or when only partial privileges are processed
- Special handling for event triggers which explicitly do not support grantable rights
- Column-level privilege operations receive specialized error messages that include both column and relation names
- The function enforces PostgreSQL's security model by ensuring users can only grant privileges they themselves possess with grant option
- Warning behavior differs slightly from SQL standard for REVOKE operations to reduce noise while maintaining consistency with GRANT operations
- Supports all PostgreSQL object types that have ACL-based security including tables, functions, schemas, tablespaces, foreign data wrappers, and configuration parameters