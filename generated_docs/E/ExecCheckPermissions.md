# ExecCheckPermissions

## Location
src/backend/executor/execMain.c: 572 - 635

## Overview
Validates access permissions for all relations mentioned in a query, with support for plugin hooks and optional error reporting.

## Definition


## Detailed Description
The  function performs comprehensive access control validation for all relations referenced in a query execution plan. It iterates through the permission information structures (rteperminfos) and validates each relation's access permissions based on the current user's privileges. The function supports both strict error reporting mode and silent validation mode, making it suitable for different execution contexts.

The function includes extensive assertion checking to ensure consistency between the range table and permission info structures. It also provides a hook mechanism for plugins to implement custom permission checking logic while maintaining compatibility with the standard PostgreSQL access control system. Note that this function only handles table-level permissions and does not address row-level security policies.

## Parameters / Member Variables
- : List of RangeTblEntry structures representing the tables/relations in the query (kept for hook compatibility)
- : List of RTEPermissionInfo structures containing permission requirements for each relation
- : Boolean flag indicating whether to throw an error on permission violation (true) or return false silently

## Dependencies
- Functions called/Symbols referenced:
  - getRTEPermissionInfo
  - ExecCheckOneRelPerms
  - aclcheck_error
  - get_relkind_objtype
  - get_rel_relkind
  - get_rel_name
  - bms_is_member
  - bms_add_member
  - bms_num_members
  - ExecutorCheckPerms_hook (function pointer)
- Called from (representative examples):
  - DoCopy
  - InitPlan
  - RI_Initial_Check

## Notes and Other Information
- Does NOT address row-level security (RLS) policies - separate RLS checking is required
- Provides hook mechanism (ExecutorCheckPerms_hook) for plugin extensibility
- rangeTable parameter is kept mainly for backward compatibility with hooks
- Includes comprehensive assertion checking in debug builds to validate data structure consistency
- Only relation RTEs and view RTEs (subquery RTEs that were once relation RTEs) have permission info
- Returns true if all permissions are adequate, false otherwise
- When ereport_on_violation is true, throws appropriate ACL errors instead of returning false
- Critical for enforcing PostgreSQL's table-level access control security model