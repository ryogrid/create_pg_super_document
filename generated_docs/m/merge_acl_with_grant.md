# merge_acl_with_grant

## Location
src/backend/catalog/aclchk.c: 182 - 240

## Overview
Merges ACL (Access Control List) entries by either adding or removing specified privileges for given grantees, modifying an existing ACL structure.

## Definition


## Detailed Description
This static function performs the core ACL modification logic for PostgreSQL's GRANT and REVOKE operations. It takes an existing ACL and either adds privileges (when is_grant is true) or removes privileges (when is_grant is false) for a list of grantees. The function iterates through each grantee in the list, creates an AclItem structure for each one, and calls aclupdate() to perform the actual ACL modification. The function handles the asymmetric semantics between GRANT and REVOKE operations as specified by SQL standards - GRANT WITH GRANT OPTION grants both basic privileges and grant options, while REVOKE removes both unless specifically revoking only the grant option. The original old_acl is freed to prevent memory leaks.

## Parameters / Member Variables
- : The existing ACL structure to be modified (will be freed by this function)
- : Boolean flag indicating whether this is a GRANT (true) or REVOKE (false) operation
- : Boolean flag indicating whether grant options are being manipulated
- : DropBehavior enum specifying how to handle dependencies during privilege removal
- : List of OIDs representing the users/roles to grant privileges to or revoke from
- : AclMode bitmask representing the specific privileges being granted or revoked
- : OID of the user/role performing the grant or revoke operation
- : OID of the object owner (used in aclupdate for privilege validation)

## Dependencies
- Functions called/Symbols referenced:
  - aclupdate
  - ACLITEM_SET_PRIVS_GOPTIONS
  - pfree
  - lfirst_oid
  - ereport
- Types used:
  - Acl
  - AclItem
  - AclMode
  - DropBehavior
- Constants referenced:
  - ACL_MODECHG_ADD
  - ACL_MODECHG_DEL
  - ACL_ID_PUBLIC
  - ACL_NO_RIGHTS
- Called from:
  - SetDefaultACL
  - ExecGrant_Attribute
  - ExecGrant_Relation
  - ExecGrant_common
  - ExecGrant_Largeobject
  - ExecGrant_Parameter
  - RemoveRoleFromInitPriv

## Notes and Other Information
- The function enforces a critical security restriction: grant options can only be granted to individual roles, not to PUBLIC, to prevent uncleanable privilege escalation scenarios
- Memory management is carefully handled - the original old_acl is freed and intermediate ACLs are freed during iteration to prevent leaks when processing multiple grantees
- The function implements the SQL standard's asymmetric GRANT/REVOKE semantics where GRANT WITH GRANT OPTION affects both privileges and grant options, while REVOKE has different behavior for regular revoke vs REVOKE GRANT OPTION
- This is a core utility function used by all PostgreSQL object types that support ACL-based permissions