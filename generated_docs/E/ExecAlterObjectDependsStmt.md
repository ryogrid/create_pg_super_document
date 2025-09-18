# ExecAlterObjectDependsStmt

## Location
[src/backend/commands/alter.c:457-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L457-L520)

## Overview
Executes ALTER OBJECT [NO] DEPENDS ON EXTENSION statements to create or remove automatic extension dependencies between database objects and extensions.

## Definition
```c
ObjectAddress ExecAlterObjectDependsStmt(AlterObjectDependsStmt *stmt, ObjectAddress *refAddress)
```

## Detailed Description
This function handles SQL statements that establish or remove dependency relationships between database objects and extensions using the DEPENDENCY_AUTO_EXTENSION dependency type. When an object depends on an extension with this dependency type, the object can be automatically dropped when the extension is dropped. The function resolves both the target object and the referenced extension, performs ownership checks, and either creates or removes the dependency record as specified. It prevents duplicate dependencies by checking existing auto-extension relationships before creating new ones.

## Parameters / Member Variables
- `stmt`: Pointer to AlterObjectDependsStmt structure containing the operation details including object type, object identifier, extension name, and whether to add or remove the dependency
- `refAddress`: Output parameter (can be NULL) that receives the address of the extension object that the altered object now depends on or previously depended on

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_address_rv](../g/get_object_address_rv.md) (resolve object with relation support)
  - [get_object_address](../g/get_object_address.md) (resolve extension object)
  - [check_object_ownership](../c/check_object_ownership.md) (verify user can modify the object)
  - table_close (release relation locks)
  - [deleteDependencyRecordsForSpecific](../d/deleteDependencyRecordsForSpecific.md) (remove specific dependency records)
  - [getAutoExtensionsOfObject](../g/getAutoExtensionsOfObject.md) (get existing auto-extension dependencies)
  - [list_member_oid](../l/list_member_oid.md) (check for duplicate dependencies)
  - [recordDependencyOn](../r/recordDependencyOn.md) (create new dependency record)
  - AccessExclusiveLock, NoLock (locking modes)
  - OBJECT_EXTENSION (object type constant)
  - DEPENDENCY_AUTO_EXTENSION (dependency type constant)

- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:1009)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1782)

## Notes and Other Information
- Public function (not static), part of the command execution interface
- Declared in src/include/commands/alter.h:23
- Handles both adding and removing dependencies based on the stmt->remove flag
- Uses AccessExclusiveLock for both the target object and extension to ensure consistency
- No special privileges are required on the extension since the object owner is explicitly allowing the extension owner to drop the object
- Prevents creation of duplicate auto-extension dependencies through getAutoExtensionsOfObject check
- Returns the ObjectAddress of the altered object, allowing callers to track what was modified
- Part of PostgreSQL's extension dependency management system
- Maintains proper locking protocol: acquires locks during resolution, releases relation locks but retains others until commit
- The dependency type DEPENDENCY_AUTO_EXTENSION allows automatic cleanup when extensions are dropped