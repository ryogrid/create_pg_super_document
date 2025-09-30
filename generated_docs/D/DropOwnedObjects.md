# DropOwnedObjects

## Location
[src/backend/commands/user.c:1583-1610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L1583-L1610)

## Overview
Implements the DROP OWNED command by dropping all objects owned by specified roles after performing appropriate privilege checks.

## Definition

```c
void
DropOwnedObjects(DropOwnedStmt *stmt)
```
## Detailed Description
DropOwnedObjects is the high-level interface for PostgreSQL's DROP OWNED command, which removes all database objects owned by one or more specified roles. The function performs essential authorization checks to ensure the current user has the necessary privileges to drop objects owned by the target roles, then delegates the actual dropping operation to the lower-level shdepDropOwned function.

The function serves as a security wrapper around the shared dependency system's object dropping functionality, ensuring that only users with appropriate privileges can remove objects owned by other roles. This is crucial for maintaining database security and preventing unauthorized data deletion.

## Parameters / Member Variables
- : DropOwnedStmt containing the list of roles whose objects should be dropped and the drop behavior (CASCADE or RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - [roleSpecsToIds](../r/roleSpecsToIds.md): Convert role specifications to OID list
  - [has_privs_of_role](../h/has_privs_of_role.md): Check if current user has privileges of target role
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

## Simplified Source

```c
void
DropOwnedObjects(DropOwnedStmt *stmt)
{
    List *role_ids;
    ListCell *cell;

    // Convert role specifications to OID list
    role_ids = roleSpecsToIds(stmt->roles);

    // Check privileges for each role
    foreach(cell, role_ids) {
        Oid roleid = lfirst_oid(cell);

        if (!has_privs_of_role(GetUserId(), roleid))
            ereport(ERROR,
                   (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("permission denied to drop objects"),
                    errdetail("Only roles with privileges of role \"%s\" may drop objects owned by it.",
                             GetUserNameFromId(roleid, false))));
    }

    // Perform the actual object dropping
    shdepDropOwned(role_ids, stmt->behavior);
}
```