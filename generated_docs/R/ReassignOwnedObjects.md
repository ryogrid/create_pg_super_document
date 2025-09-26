# ReassignOwnedObjects

## Location
[src/backend/commands/user.c:1611-1651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1611-L1651)

## Overview
Implements the REASSIGN OWNED command by transferring ownership of all objects owned by specified roles to a new target role after validating privileges.

## Definition

```c
void
ReassignOwnedObjects(ReassignOwnedStmt *stmt)
```
## Detailed Description
ReassignOwnedObjects provides the high-level interface for PostgreSQL's REASSIGN OWNED command, which transfers ownership of all database objects from one or more source roles to a single target role. The function performs comprehensive privilege validation on both the source and destination sides before delegating the actual ownership transfer to the lower-level shdepReassignOwned function.

This function serves as a security-aware wrapper that ensures only authorized users can perform ownership transfers. It requires the executing user to have privileges of both the source roles (whose objects are being transferred) and the target role (which will receive ownership). This dual-privilege requirement prevents unauthorized ownership changes that could compromise database security.

## Parameters / Member Variables
- : ReassignOwnedStmt containing the list of source roles whose objects should be reassigned and the target role that will receive ownership

## Dependencies
- Functions called/Symbols referenced:
  - [roleSpecsToIds](../r/roleSpecsToIds.md): Convert source role specifications to OID list
  - [get_rolespec_oid](../g/get_rolespec_oid.md): Convert target role specification to OID
  - [has_privs_of_role](../h/has_privs_of_role.md): Check if current user has privileges of specified role (used for both source and target validation)
  - [GetUserNameFromId](../G/GetUserNameFromId.md): Get role name for error messages
  - [shdepReassignOwned](../s/shdepReassignOwned.md): Perform the actual ownership transfer operation
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility command processing

## Notes and Other Information
- Requires dual privilege validation: current user must have privileges of both source roles and the target role
- Ownership is transferred across all databases where the source roles own objects
- Unlike DROP OWNED, this operation preserves objects while changing their ownership
- The operation is atomic and either succeeds completely or fails without partial changes
- Related to the already documented shdepReassignOwned function which performs the lower-level reassignment logic
- Commonly used during role cleanup or reorganization scenarios where objects need to be preserved under new ownership