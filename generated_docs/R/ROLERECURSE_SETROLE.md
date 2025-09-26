# ROLERECURSE_SETROLE

## Location
src/backend/utils/adt/acl.c: 76 - 87

## Overview
ROLERECURSE_SETROLE is an enumeration constant that specifies role membership recursion through grants with the SET ROLE option, allowing role membership traversal only through role grants that explicitly permit SET ROLE privilege.

## Definition


## Detailed Description
ROLERECURSE_SETROLE is one of three enumeration values in the RoleRecurseType enum that control how PostgreSQL traverses role membership hierarchies. This specific constant instructs the role membership checking functions to only follow role grants that have the  flag enabled, meaning the member role can use SET ROLE to assume the identity of the granted role.

This recursion type is specifically used in security-sensitive contexts where only roles that can actually be assumed (via SET ROLE) should be considered in the membership chain. It provides a more restrictive membership check compared to ROLERECURSE_MEMBERS (which follows all grants unconditionally) and is distinct from ROLERECURSE_PRIVS (which follows inheritable privilege grants).

The enum serves as a parameter to role membership functions and as an array index for caching role membership information. Each recursion type maintains its own cached list of roles to optimize repeated membership queries.

## Parameters / Member Variables
- Value:  (integer constant in the RoleRecurseType enumeration)
- Used as array index for  and  arrays
- Controls role membership traversal behavior in  function

## Dependencies
- **Used by functions:**
  -  - Invalidates cached role membership when role grants change
  -  - Core function that traverses role membership hierarchies
  -  - Checks if a member role can SET ROLE to a target role

- **Related symbols:**
  -  - Alternative recursion type (unconditional)
  -  - Alternative recursion type (inheritable grants)
  -  - Parent enumeration type
  -  - Static array for caching role OIDs
  -  - Static array for caching role membership lists

## Notes and Other Information
- The enumeration is defined in src/backend/utils/adt/acl.c:76 as part of PostgreSQL's access control implementation
- Used primarily for SET ROLE permission checking, which is a security-critical operation that allows users to assume different role identities
- The caching mechanism improves performance by avoiding repeated role membership traversals for the same role combinations
- When role membership changes occur, the cache entry for this recursion type is invalidated by setting 
- The distinction between this and other recursion types is crucial for proper privilege separation and security boundaries in PostgreSQL's role-based access control system