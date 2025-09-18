# DropOwnedObjects

## Location
src/backend/commands/user.c: 1583 - 1610

## Overview
Implements the DROP OWNED command by dropping all objects owned by specified roles after performing appropriate privilege checks.

## Definition


## Detailed Description
DropOwnedObjects is the high-level interface for PostgreSQL's DROP OWNED command, which removes all database objects owned by one or more specified roles. The function performs essential authorization checks to ensure the current user has the necessary privileges to drop objects owned by the target roles, then delegates the actual dropping operation to the lower-level shdepDropOwned function.

The function serves as a security wrapper around the shared dependency system's object dropping functionality, ensuring that only users with appropriate privileges can remove objects owned by other roles. This is crucial for maintaining database security and preventing unauthorized data deletion.

## Parameters / Member Variables
- : DropOwnedStmt containing the list of roles whose objects should be dropped and the drop behavior (CASCADE or RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - [roleSpecsToIds](../r/roleSpecsToIds.md): Convert role specifications to OID list
  - has_privs_of_role: Check if current user has privileges of target role
  - [GetUserNameFromId](../G/GetUserNameFromId.md): Get role name for error messages
  - [shdepDropOwned](../s/shdepDropOwned.md): Perform the actual object dropping operation
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processing

## Notes and Other Information
- Requires the current user to have privileges of each target role (typically through role membership or superuser status)
- The drop behavior from the statement (CASCADE or RESTRICT) is passed through to shdepDropOwned
- Objects are dropped across all databases where the roles have ownership
- This is a potentially destructive operation that should be used with caution
- Related to the already documented shdepDropOwned function which performs the lower-level dropping logic