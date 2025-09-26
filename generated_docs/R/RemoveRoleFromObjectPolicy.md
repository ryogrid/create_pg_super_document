# RemoveRoleFromObjectPolicy

## Location
[src/backend/commands/policy.c:416-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L416-L568)

## Overview
Removes a specified role from a policy's applicable roles list, maintaining policy integrity by either updating the policy with remaining roles or indicating the policy should be dropped if no roles would remain.

## Definition

```c
struct_array_builtin(role_oids, num_roles, OIDOID);
```
## Detailed Description
This function removes a role from a policy's applicable roles list stored in the pg_policy catalog. It handles several important aspects:

1. **Role Removal**: Scans through the policy's current roles array and rebuilds it without the target role
2. **Duplicate Handling**: Properly handles cases where the same role appears multiple times in the policy (historically allowed by CREATE/ALTER POLICY)
3. **Policy Preservation Logic**: Returns a boolean indicating whether the policy should be kept or dropped entirely
4. **Dependency Management**: Updates shared dependency records to reflect the new set of applicable roles
5. **Cache Invalidation**: Invalidates relation cache for the table the policy belongs to, forcing plan recompilation

The function performs atomic operations on the pg_policy system catalog and maintains referential integrity through the dependency system.

## Parameters
- : OID of the role to be removed from the policy
- : Should always be PolicyRelationId (assertion enforced)  
- : OID of the policy from which to remove the role

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (catalog scanning)
  - [heap_getattr](../h/heap_getattr.md), heap_modify_tuple, heap_freetuple (tuple manipulation)
  - DatumGetArrayTypePCopy, construct_array_builtin (array operations)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md), recordSharedDependencyOn (dependency management)
  - InvokeObjectPostAlterHook (event hooks)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md), CacheInvalidateRelcacheByTuple (cache management)
- Called from:
  - [shdepDropOwned](../s/shdepDropOwned.md) (when dropping owned objects during role cleanup)

## Notes and Other Information
- Returns true if the policy should be kept (roles remain), false if it should be dropped (no roles left)
- The caller is responsible for actually dropping the policy when this function returns false
- Uses RowExclusiveLock on pg_policy to prevent concurrent modifications
- Handles race conditions gracefully (e.g., if the relation was dropped concurrently)
- Does not create dependencies on the PUBLIC role (ACL_ID_PUBLIC) as it's implicitly available