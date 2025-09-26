# is_admin_of_role

## Location
src/backend/utils/adt/acl.c: 5281 - 5305

## Overview
Determines whether a given user/role has administrative privileges over another role, meaning they can manage that role's membership and properties.

## Definition


## Detailed Description
This function checks if a member has administrative privileges over a target role. Administrative privileges are granted in the following cases:

1. **Superuser privileges**: Superusers have admin rights over all roles
2. **WITH ADMIN OPTION**: The member is granted membership in the role with the ADMIN OPTION flag
3. **Indirect admin membership**: The member inherits admin privileges through role membership chains

**Important Policy Restriction**: A role cannot have WITH ADMIN OPTION on itself - this is explicitly forbidden by PostgreSQL policy to prevent certain security issues.

The function uses `roles_is_member_of` to traverse the role hierarchy and check for admin-level membership, returning the specific admin role that grants the privileges.

## Parameters / Member Variables
- `member`: The OID of the user/role being tested for administrative privileges
- `role`: The OID of the target role to check administrative access against

## Dependencies
- Functions called/Symbols referenced:
  - `superuser_arg`: Checks if the member has superuser privileges
  - `roles_is_member_of`: Recursively searches role membership with admin role tracking
  - `ROLERECURSE_MEMBERS`: Constant controlling recursion behavior
  - `OidIsValid`: Macro to validate OID values
- Called from (representative examples):
  - `check_object_ownership`: Object ownership validation
  - `AlterRole`: Role alteration commands  
  - `DropRole`: Role deletion operations
  - `RenameRole`: Role renaming functionality
  - `pg_role_aclcheck`: Access control checking

## Notes and Other Information
- Returns `false` immediately if member equals role (self-admin prevention policy)
- Superusers automatically have admin privileges over all roles
- Uses an output parameter in `roles_is_member_of` to identify which specific role grants admin privileges
- This function is crucial for PostgreSQL's role-based security model and administrative operations
- The admin role tracking allows for precise privilege attribution in complex role hierarchies
- Located in `src/backend/utils/adt/acl.c:5281-5305`