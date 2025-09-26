# InitGrantRoleOptions

## Location
src/backend/commands/user.c: 2503 - 2514

## Overview
Initializes a GrantRoleOptions structure with default values for role grant operations.

## Definition
```c
static void InitGrantRoleOptions(GrantRoleOptions *popt)
```

## Detailed Description
This utility function sets up a GrantRoleOptions structure with PostgreSQL's default values for role grant operations. The function ensures consistent initialization across different contexts where role grants are processed. The default configuration reflects PostgreSQL's standard behavior: no specific options are set (specified = 0), admin and inherit privileges are disabled by default, but the SET option is enabled by default to allow role switching.

This initialization is crucial for maintaining consistent behavior across role creation, alteration, and granting operations.

## Parameters / Member Variables
- `popt`: Pointer to a GrantRoleOptions structure to be initialized

## Dependencies
- Types used:
  - GrantRoleOptions
- Called from:
  - CreateRole
  - AlterRole
  - GrantRole

## Notes and Other Information
- The `specified` field is set to 0, meaning no specific options have been explicitly set
- The `admin` field defaults to false, meaning no administrative privileges are granted by default
- The `inherit` field defaults to false, meaning role inheritance is disabled by default
- The `set` field defaults to true, allowing the grantee to SET ROLE to the granted role
- This function provides the baseline configuration that can be modified by parsing user-specified options
- Used consistently across role management operations to ensure uniform default behavior
- The function is static and only accessible within the user.c module
- Simple but essential for maintaining consistent role option semantics throughout the system