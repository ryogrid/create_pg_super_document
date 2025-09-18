# check_circularity

## Location
src/backend/utils/adt/acl.c: 1222 - 1301

## Overview
Prevents circular chains of grant options by verifying that granting grant options would not create a situation where privileges cannot be effectively revoked.

## Definition
```c
static void check_circularity(const Acl *old_acl, const AclItem *mod_aip, Oid ownerId)
```

## Detailed Description
This function implements a critical security check in PostgreSQL's privilege system to prevent circular grant option chains. When a user attempts to grant privileges with grant options, this function ensures that the resulting configuration would not create a circular dependency that would make it impossible for the object owner to effectively revoke privileges. The check works by simulating the removal of all grant options belonging to the target grantee (and their dependencies) from a working copy of the ACL, then verifying that the would-be grantor still independently possesses the necessary grant options to perform the grant operation.

## Parameters / Member Variables
- `old_acl` (const Acl *): The current ACL array before the proposed grant operation
- `mod_aip` (const AclItem *): The ACL item describing the proposed grant, including grantee, grantor, and privileges with grant options
- `ownerId` (Oid): Object identifier of the object owner (who always has implicit grant options)

## Dependencies
- Functions called/Symbols referenced:
  - check_acl: Validates ACL structure
  - allocacl: Allocates memory for working copy of ACL
  - aclupdate: Updates ACL by removing grant options (called recursively via goto)
  - aclmask: Computes effective privileges for the grantor
  - ACL manipulation macros (ACL_NUM, ACL_DAT, ACLITEM_GET_GOPTIONS, etc.)
  - Memory management functions (memcpy, pfree)
- Called from (representative examples):
  - aclupdate: During grant option operations to prevent circular dependencies

## Notes and Other Information
- Static function, only accessible within the ACL module
- Currently only supports role-based grantees, not PUBLIC (asserted at runtime)
- Object owners always have grant options, so no check is needed for owner grants
- Uses a restart mechanism (goto cc_restart) to handle cascading removals
- Throws ERROR with ERRCODE_INVALID_GRANT_OPERATION if circularity is detected
- The algorithm simulates privilege revocation to test for independence of grant options
- Essential for maintaining the integrity of PostgreSQL's privilege revocation system
- Located in src/backend/utils/adt/acl.c:1222-1301