# shdepDropOwned

## Location
src/backend/catalog/pg_shdepend.c: 1342 - 1529

## Overview
Drops all objects owned by the specified role(s) and removes any access grants the role(s) have on other objects. This function is used during role deletion to clean up all dependencies.

## Definition


## Detailed Description
The shdepDropOwned function scans the pg_shdepend catalog to find all objects that depend on the given roles and handles them according to their dependency type. It performs the following operations:

1. **Ownership Dependencies**: Objects owned by the role are collected for deletion
2. **ACL Dependencies**: Access grants to the role are removed from object ACLs  
3. **Policy Dependencies**: The role is removed from row-level security policies, or the entire policy is deleted if removal fails
4. **Initial ACL Dependencies**: References in pg_init_privs are cleaned up

The function uses a two-phase approach: grants and policy modifications are handled immediately during the scan, while object deletions are deferred and performed in batch using performMultipleDeletions to avoid dependency ordering issues.

## Parameters / Member Variables
- : List of role OIDs to process for owned object deletion
- : DropBehavior enum controlling cascade vs restrict semantics for deletions

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md) - Creates ObjectAddresses collection for batch deletion
  - table_open/table_close - Opens pg_shdepend catalog with RowExclusiveLock
  - [systable_beginscan](systable_beginscan.md)/systable_getnext/systable_endscan - Scans pg_shdepend entries
  - [IsPinnedObject](../I/IsPinnedObject.md) - Checks if role is system-critical and cannot be dropped
  - [RemoveRoleFromObjectPolicy](../R/RemoveRoleFromObjectPolicy.md) - Attempts to remove role from RLS policy
  - [RemoveRoleFromObjectACL](../R/RemoveRoleFromObjectACL.md) - Removes role from object's ACL
  - [RemoveRoleFromInitPriv](../R/RemoveRoleFromInitPriv.md) - Cleans up pg_init_privs entries
  - [AcquireDeletionLock](../A/AcquireDeletionLock.md)/ReleaseDeletionLock - Manages object locking for deletion
  - [systable_recheck_tuple](systable_recheck_tuple.md) - Verifies tuple validity after lock acquisition
  - [add_exact_object_address](../a/add_exact_object_address.md) - Adds object to deletion list
  - [sort_object_addresses](sort_object_addresses.md) - Orders objects for stable deletion sequence
  - [performMultipleDeletions](../p/performMultipleDeletions.md) - Executes batch deletion with dependency resolution
  - [free_object_addresses](../f/free_object_addresses.md) - Cleans up ObjectAddresses structure

- Called from (representative examples):
  - [DropOwnedObjects](../D/DropOwnedObjects.md) (src/backend/commands/user.c:1602)

## Notes and Other Information
- Protected against dropping pinned system objects by checking IsPinnedObject
- Only processes objects in the current database or shared objects (dbid filtering)
- Uses systable_recheck_tuple to handle concurrent modifications during processing
- Sorting objects before deletion provides stable error reporting and may improve performance
- Handles different shared dependency types (OWNER, ACL, POLICY, INITACL) with type-specific logic
- Part of the role management infrastructure, typically called during DROP OWNED BY operations