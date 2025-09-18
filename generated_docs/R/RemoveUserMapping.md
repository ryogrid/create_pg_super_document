# RemoveUserMapping

## Location
src/backend/commands/foreigncmds.c: 1335 - 1414

## Overview
Removes an existing user mapping from the system catalog, effectively deleting the authentication and connection information for a specific user's access to a foreign server.

## Definition
```c
Oid RemoveUserMapping(DropUserMappingStmt *stmt)
```

## Detailed Description
This function implements the DROP USER MAPPING SQL command by removing an entry from the pg_user_mapping system catalog. It handles the complete removal of user-to-foreign-server authentication mappings with comprehensive error handling and support for conditional deletion. The function performs thorough validation including existence checks for both the user/role and the foreign server, permission verification through the same access control mechanisms as other user mapping operations, and graceful handling of missing objects when the IF EXISTS clause is used. Upon successful validation, it delegates the actual deletion to PostgreSQL's standard object deletion framework with CASCADE semantics.

## Parameters / Member Variables
- `stmt`: Pointer to DropUserMappingStmt structure containing the parsed DROP USER MAPPING command details including user specification, server name, and conditional flags like IF EXISTS

## Dependencies
- Functions called/Symbols referenced:
  - get_rolespec_oid
  - [GetForeignServerByName](../G/GetForeignServerByName.md)
  - GetSysCacheOid2
  - MappingUserName
  - [user_mapping_ddl_aclcheck](../u/user_mapping_ddl_aclcheck.md)
  - [performDeletion](../p/performDeletion.md)
  - DROP_CASCADE
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
The function supports graceful degradation with the IF EXISTS clause, issuing notices instead of errors when the specified user mapping, server, or role does not exist. It uses CASCADE deletion semantics to ensure proper cleanup of any dependent objects. The function returns the OID of the deleted mapping on success, or InvalidOid when the operation is skipped due to missing objects in conditional mode. Access control is enforced through the same user_mapping_ddl_aclcheck function used by other user mapping operations.