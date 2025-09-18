# ExecAlterExtensionContentsStmt

## Location
src/backend/commands/extension.c: 3292 - 3377

## Overview
Executes ALTER EXTENSION ADD/DROP commands to modify the contents of an extension by adding or removing database objects.

## Definition


## Detailed Description
This function implements the core logic for ALTER EXTENSION ADD/DROP SQL commands. It validates that the specified object type can be added to extensions, resolves both the extension and target object addresses, performs necessary permission checks, and delegates to a recursive helper function to handle the actual modification and any dependent objects. The function ensures proper concurrency control through strategic locking and maintains referential integrity throughout the operation.

Key operations include:
1. Validating that the object type is eligible for extension membership
2. Acquiring appropriate locks on both the extension and target object
3. Performing ownership verification for both the extension and target object  
4. Delegating to ExecAlterExtensionContentsRecurse for the actual modification
5. Triggering post-alter hooks and cleaning up resources

## Parameters / Member Variables
- : AlterExtensionContentsStmt structure containing the parsed command details including extension name, object type, object specification, and operation type (ADD/DROP)
- : Output parameter that receives the ObjectAddress of the added/dropped object if not NULL

## Dependencies
- Functions called/Symbols referenced:
  - get_object_address
  - object_ownercheck
  - aclcheck_error
  - check_object_ownership
  - ExecAlterExtensionContentsRecurse
  - InvokeObjectPostAlterHook
  - relation_close
  - makeString
- Called from (representative examples):
  - ProcessUtilitySlow (main utility command processor)

## Notes and Other Information
- Certain object types (DATABASE, EXTENSION, INDEX, PUBLICATION, ROLE, STATISTIC_EXT, SUBSCRIPTION, TABLESPACE) are explicitly prohibited from being added to extensions
- Uses AccessShareLock for the extension to allow concurrent operations while preventing drops
- Uses ShareUpdateExclusiveLock for the target object to prevent concurrent modifications
- Returns the ObjectAddress of the modified extension
- Maintains proper lock ordering and cleanup to prevent deadlocks and resource leaks
- The function is part of the DDL utility command processing pipeline