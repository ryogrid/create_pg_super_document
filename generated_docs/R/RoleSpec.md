# RoleSpec

## Location
src/include/nodes/parsenodes.h: 401 - 407

## Overview
RoleSpec represents a role specification in PostgreSQL's parse tree, used to identify database roles (users/groups) in various SQL statements involving permissions, ownership, and role management.

## Definition


## Detailed Description
RoleSpec is a parse tree node that represents role specifications in SQL statements. It provides a flexible way to reference database roles, supporting different methods of role identification including explicit role names, current user references, session user references, and public role references. This abstraction allows PostgreSQL to handle various role reference patterns uniformly across different SQL contexts such as GRANT statements, role creation, ownership changes, and access control operations.

## Parameters / Member Variables
- : NodeTag identifying this as a RoleSpec node
- : RoleSpecType enum indicating how the role is specified (e.g., explicit name, current user, session user, public)
- : Character pointer containing the role name, populated only when roletype is ROLESPEC_CSTRING
- : ParseLoc storing the token's position in the source SQL, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - [RoleSpecType](RoleSpecType.md)
  - ParseLoc
- Called from (representative examples):
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md)
  - [CreateRole](../C/CreateRole.md)
  - [DropRole](../D/DropRole.md)
  - [roleSpecsToIds](../r/roleSpecsToIds.md)
  - get_rolespec_oid
  - [AddRoleMems](../A/AddRoleMems.md)
  - [DelRoleMems](../D/DelRoleMems.md)
  - [CreateUserMapping](../C/CreateUserMapping.md)

## Notes and Other Information
- [RoleSpec](RoleSpec.md) provides abstraction for different ways to specify roles: by name, CURRENT_USER, SESSION_USER, or PUBLIC
- The rolename field is only meaningful when roletype is ROLESPEC_CSTRING
- Used extensively in security and access control contexts throughout PostgreSQL
- Supports role resolution functions that convert RoleSpec to actual role OIDs
- Essential for GRANT/REVOKE statements, role membership operations, and ownership assignments
- Location information enables precise error reporting for role-related syntax errors