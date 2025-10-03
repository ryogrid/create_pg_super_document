# ATPrepSetTableSpace

## Location
[src/backend/commands/tablecmds.c:15019-15048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15019-L15048)

## Overview
ATPrepSetTableSpace is a preparation function for the ALTER TABLE SET TABLESPACE command that validates the target tablespace and stores the tablespace change information for later execution during the ALTER TABLE operation.

## Definition

```c
static void
ATPrepSetTableSpace(AlteredTableInfo *tab, Relation rel, const char *tablespacename, LOCKMODE lockmode)
```
## Detailed Description
This function serves as the preparation phase for moving a table to a different tablespace as part of an ALTER TABLE operation. It performs validation checks on the target tablespace, including existence verification and permission checking, then stores the validated tablespace OID in the AlteredTableInfo structure for the actual move operation to be performed later in Phase 3 of the ALTER TABLE process.

The function follows PostgreSQL's multi-phase ALTER TABLE design where preparation functions validate and collect information, while execution functions perform the actual changes. This separation allows for proper dependency handling and transaction safety.

## Parameters / Member Variables
- `*tab`: Pointer to AlteredTableInfo structure that accumulates information about table alterations during the ALTER TABLE command
- `rel`: The relation (table) being altered
- `*tablespacename`: Name of the target tablespace to move the table to
- `lockmode`: Lock mode to be used (though not directly used in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [get_tablespace_oid](../g/get_tablespace_oid.md): Resolves tablespace name to OID
  - [object_aclcheck](../o/object_aclcheck.md): Checks user permissions on the tablespace
  - [aclcheck_error](../a/aclcheck_error.md): Reports permission-related errors
  - [AlteredTableInfo](AlteredTableInfo.md): Structure for storing table alteration information
  - [AclResult](AclResult.md): Enumeration for access control results
  - ACL_CREATE: Permission constant for CREATE privilege
  - OBJECT_TABLESPACE: Object type constant for tablespaces

- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md): Main ALTER TABLE command preparation dispatcher

## Notes and Other Information
- This function only performs validation and preparation; the actual tablespace move is handled by ATExecSetTableSpace in Phase 3
- Prevents multiple SET TABLESPACE subcommands in a single ALTER TABLE statement
- Allows moving to the database's default tablespace without special CREATE permissions
- The function validates that the target tablespace exists and the user has CREATE permission on it
- Uses MyDatabaseTableSpace global variable to identify the database's default tablespace

## Simplified Source

```c
static void
ATPrepSetTableSpace(AlteredTableInfo *tab, Relation rel, const char *tablespacename, LOCKMODE lockmode)
{
    // Resolve tablespace name to OID
    Oid tablespaceId = get_tablespace_oid(tablespacename, false);

    // Check permissions (except for database's default tablespace)
    if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace) {
        AclResult aclresult = object_aclcheck(TableSpaceRelationId, tablespaceId, GetUserId(), ACL_CREATE);

        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_TABLESPACE, tablespacename);
    }

    // Prevent multiple SET TABLESPACE commands
    if (OidIsValid(tab->newTableSpace))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("cannot have multiple SET TABLESPACE subcommands")));

    // Save for Phase 3 execution
    tab->newTableSpace = tablespaceId;
}
```