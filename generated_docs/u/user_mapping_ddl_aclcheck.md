# user_mapping_ddl_aclcheck

## Location
src/backend/commands/foreigncmds.c: 1086 - 1110

## Overview
A common utility function that performs access control checks for user-mapping-related DDL commands, ensuring that only server owners or users operating on their own mappings can perform the requested operations.

## Definition


## Detailed Description
This static function implements a centralized permission checking mechanism for user mapping DDL operations. It enforces a two-tier access control policy: server owners have full privileges to operate on any user mapping associated with their server, while regular users can only operate on their own user mappings. The function first checks if the current user owns the foreign server; if not, it verifies whether the user is attempting to operate on their own mapping and has USAGE privileges on the server.

## Parameters / Member Variables
- : The OID of the user whose mapping is being operated on
- : The OID of the foreign server associated with the user mapping
- : The name of the foreign server (used for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - GetUserId
  - object_ownercheck
  - object_aclcheck
  - aclcheck_error
  - ACL_USAGE
  - ACLCHECK_NOT_OWNER
  - OBJECT_FOREIGN_SERVER
- Called from (representative examples):
  - CreateUserMapping
  - AlterUserMapping
  - RemoveUserMapping

## Notes and Other Information
This function serves as a security gate for all user mapping DDL operations, centralizing the access control logic to ensure consistency across different commands. The function uses PostgreSQL's standard ACL (Access Control List) checking mechanisms and follows the principle of least privilege by allowing users to modify only their own mappings unless they own the entire server.