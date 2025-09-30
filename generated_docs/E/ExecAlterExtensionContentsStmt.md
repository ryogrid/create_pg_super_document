# ExecAlterExtensionContentsStmt

## Location
[src/backend/commands/extension.c:3292-3377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L3292-L3377)

## Overview
Executes ALTER EXTENSION ADD/DROP commands to modify the contents of an extension by adding or removing database objects.

## Definition

```c
ObjectAddress
ExecAlterExtensionContentsStmt(AlterExtensionContentsStmt *stmt,
							   ObjectAddress *objAddr)
```
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
  - [get_object_address](../g/get_object_address.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [check_object_ownership](../c/check_object_ownership.md)
  - [ExecAlterExtensionContentsRecurse](ExecAlterExtensionContentsRecurse.md)
  - InvokeObjectPostAlterHook
  - [relation_close](../r/relation_close.md)
  - [makeString](../m/makeString.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main utility command processor)

## Notes and Other Information
- Certain object types (DATABASE, EXTENSION, INDEX, PUBLICATION, ROLE, STATISTIC_EXT, SUBSCRIPTION, TABLESPACE) are explicitly prohibited from being added to extensions
- Uses AccessShareLock for the extension to allow concurrent operations while preventing drops
- Uses ShareUpdateExclusiveLock for the target object to prevent concurrent modifications
- Returns the ObjectAddress of the modified extension
- Maintains proper lock ordering and cleanup to prevent deadlocks and resource leaks
- The function is part of the DDL utility command processing pipeline

## Simplified Source

```c
ObjectAddress ExecAlterExtensionContentsStmt(AlterExtensionContentsStmt *stmt,
                                            ObjectAddress *objAddr)
{
    ObjectAddress extension;
    ObjectAddress object;
    Relation relation;

    // Validate object type can be added to extensions
    switch (stmt->objtype)
    {
        case OBJECT_DATABASE:
        case OBJECT_EXTENSION:
        case OBJECT_INDEX:
        case OBJECT_PUBLICATION:
        case OBJECT_ROLE:
        case OBJECT_STATISTIC_EXT:
        case OBJECT_SUBSCRIPTION:
        case OBJECT_TABLESPACE:
            ereport(ERROR, (errmsg("cannot add an object of this type to an extension")));
            break;
        default:
            break;
    }

    // Get extension and acquire lock
    extension = get_object_address(OBJECT_EXTENSION,
                                   (Node *) makeString(stmt->extname),
                                   &relation, AccessShareLock, false);

    // Check extension ownership
    if (!object_ownercheck(ExtensionRelationId, extension.objectId, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_EXTENSION, stmt->extname);

    // Get target object and acquire lock
    object = get_object_address(stmt->objtype, stmt->object,
                                &relation, ShareUpdateExclusiveLock, false);

    if (objAddr)
        *objAddr = object;

    // Check target object ownership
    check_object_ownership(GetUserId(), stmt->objtype, object,
                           stmt->object, relation);

    // Perform the add/drop operation recursively
    ExecAlterExtensionContentsRecurse(stmt, extension, object);

    // Fire post-alter hook
    InvokeObjectPostAlterHook(ExtensionRelationId, extension.objectId, 0);

    // Cleanup: close relation if opened
    if (relation != NULL)
        relation_close(relation, NoLock);

    return extension;
}
```