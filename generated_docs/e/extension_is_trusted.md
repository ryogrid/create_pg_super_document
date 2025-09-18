# extension_is_trusted

## Location
src/backend/commands/extension.c: 848 - 869

## Overview
A policy function that determines whether a given extension is trusted for installation by a non-superuser based on extension configuration and user privileges.

## Definition
```c
static bool extension_is_trusted(ExtensionControlFile *control)
```

## Detailed Description
This function implements PostgreSQL's trusted extension security policy, which allows non-superusers to install certain extensions under specific conditions. The function enforces a two-tier security check:

1. **Extension-level Trust**: The extension itself must be explicitly marked as trusted in its control file
2. **User-level Privilege**: The user must have CREATE privilege on the current database

This dual-check approach ensures that only extensions explicitly designed to be safe for non-superuser installation can be installed, and only by users who have appropriate database-level privileges. The function is part of PostgreSQL's security model that balances extensibility with database security.

The function returns true only when both conditions are satisfied, providing a conservative approach to extension installation security.

## Parameters / Member Variables
- `control`: Pointer to ExtensionControlFile containing extension metadata, including the trusted flag

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md) (checks user's ACL permissions on database)
  - MyDatabaseId (global variable for current database OID)
  - [GetUserId](../G/GetUserId.md) (gets current user's OID)
  - ACL_CREATE (permission flag for CREATE privilege)
  - DatabaseRelationId (system catalog relation ID)
- Called from:
  - [execute_extension_script](execute_extension_script.md)

## Notes and Other Information
- This is a static function within the extension.c module
- The function implements a conservative security model - both extension trust and user privileges must be satisfied
- The trusted flag in the extension control file is set by extension authors who have designed their extensions to be safe for non-superuser installation
- CREATE privilege on the database is required, not just USAGE or other lower-level privileges
- This function is part of PostgreSQL's broader trusted extension framework introduced to allow safer extension installation
- The comment mentions that error hint logic should be updated if this policy changes, indicating this function's role in security messaging