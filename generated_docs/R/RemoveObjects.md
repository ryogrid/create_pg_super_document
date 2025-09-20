# RemoveObjects

## Location
[src/backend/commands/dropcmds.c:53-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dropcmds.c#L53-L138)

## Overview
RemoveObjects is the main function that handles dropping multiple objects of various types in PostgreSQL, implementing the DROP command functionality for objects like functions, types, domains, etc.

## Definition

```c
void
RemoveObjects(DropStmt *stmt)
```
## Detailed Description
RemoveObjects processes DROP statements by looking up all specified objects first, then deleting them in a single batch operation through performMultipleDeletions(). This approach avoids unnecessary DROP RESTRICT errors when dependencies exist between the objects being dropped. The function handles object address resolution, permission checking, special cases (like preventing DROP FUNCTION on aggregates), and manages temporary namespace access tracking.

## Parameters / Member Variables
- : DropStmt pointer containing the DROP statement details including:
  - : List of objects to be dropped
  - : Type of objects being removed (OBJECT_FUNCTION, etc.)
  - : Whether to skip missing objects without error
  - : Drop behavior (CASCADE or RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md): Creates new ObjectAddresses structure
  - [get_object_address](../g/get_object_address.md): Resolves object names to ObjectAddress
  - [does_not_exist_skipping](../d/does_not_exist_skipping.md): Issues NOTICE for missing objects when missing_ok is true
  - [get_func_prokind](../g/get_func_prokind.md): Checks if function is an aggregate
  - [get_object_namespace](../g/get_object_namespace.md): Gets namespace ID for permission checks
  - [object_ownercheck](../o/object_ownercheck.md): Checks namespace ownership
  - [check_object_ownership](../c/check_object_ownership.md): Verifies user has drop permissions
  - [isTempNamespace](../i/isTempNamespace.md): Checks if namespace is temporary
  - [add_exact_object_address](../a/add_exact_object_address.md): Adds object to deletion list
  - [performMultipleDeletions](../p/performMultipleDeletions.md): Performs the actual deletions
  - [free_object_addresses](../f/free_object_addresses.md): Cleans up ObjectAddresses structure

- Called from (representative examples):
  - ExecDropStmt: Main utility command execution

## Notes and Other Information
- Does not handle relation drops (tables, indexes) which require special locking
- Prevents DROP FUNCTION from being used on aggregate functions
- Tracks temporary namespace access for transaction flags
- Maintains exclusive locks until transaction commit
- Uses batch deletion to handle inter-object dependencies correctly