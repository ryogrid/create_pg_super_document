# aclmask_direct

## Location
src/backend/utils/adt/acl.c: 1477 - 1539

## Overview
Computes the bitmask of privileges held directly by a role ID, excluding privileges inherited through role membership.

## Definition


## Detailed Description
The  function is a specialized version of  that only considers privileges granted directly to the specified role, not those inherited via role membership. This function is critical for scenarios where inheritance should be ignored, such as when determining who can serve as a grantor for privilege operations.

Unlike , this function does not check privileges granted to PUBLIC and does not perform the expensive  checks for role membership. It only examines ACL entries where the grantee exactly matches the specified .

The function maintains the same owner privilege handling as , where owners implicitly have all grant options, but only if  exactly equals  (no inheritance check).

## Parameters / Member Variables
- : The Access Control List to examine for privileges
- : The OID of the role whose direct privileges are being checked
- : The OID of the object owner (for implicit owner privileges)
- : Bitmask specifying which privileges to check for
- : Query mode -  (check all privileges) or  (early exit on any match)

## Dependencies
- Functions called/Symbols referenced:
  - [check_acl](../c/check_acl.md)
  - ACL_NUM
  - ACL_DAT
  - ACLITEM_ALL_GOPTION_BITS
  - ACLMASK_ALL
- Called from (representative examples):
  - select_best_grantor

## Notes and Other Information
- Static function, only used internally within the ACL module
- Does not check PUBLIC privileges, unlike 
- Does not perform role membership inheritance checks for performance
- Used primarily in privilege granting scenarios where direct grants matter
- Returns 0 immediately if mask is 0 or if ACL is NULL (with error)
- Critical for determining valid grantors in the privilege system