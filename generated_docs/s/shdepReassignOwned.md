# shdepReassignOwned

## Location
[src/backend/catalog/pg_shdepend.c:1530-1646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1530-L1646)

## Overview
Reassigns ownership of all objects owned by the specified role(s) to a new role. Unlike shdepDropOwned, this function transfers ownership rather than deleting objects and does not modify grants.

## Definition
```c
void shdepReassignOwned(List *roleids, Oid newrole)
```

## Detailed Description
The shdepReassignOwned function scans the pg_shdepend catalog to find all objects that have ownership dependencies on the given roles and transfers ownership to the specified new role. It processes different types of shared dependencies:

1. **SHARED_DEPENDENCY_OWNER**: Calls shdepReassignOwned_Owner to transfer object ownership
2. **SHARED_DEPENDENCY_INITACL**: Calls shdepReassignOwned_InitAcl to update initial privileges
3. **Other dependency types**: ACL, POLICY, and TABLESPACE dependencies are ignored as they don't involve ownership

The function includes memory management optimization by creating short-lived memory contexts for each object processed, preventing memory leaks when processing large numbers of objects. Each iteration calls CommandCounterIncrement to ensure changes are visible to subsequent operations.

## Parameters / Member Variables
- `roleids`: List of role OIDs whose owned objects should be reassigned 
- `newrole`: OID of the role that will become the new owner of the objects

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close - Opens pg_shdepend catalog with RowExclusiveLock
  - [IsPinnedObject](../I/IsPinnedObject.md) - Checks if role is system-critical and cannot be processed
  - [getObjectDescription](../g/getObjectDescription.md) - Generates error message descriptions
  - [systable_beginscan](systable_beginscan.md)/systable_getnext/systable_endscan - Scans pg_shdepend entries
  - AllocSetContextCreate/MemoryContextDelete - Manages memory contexts for leak prevention
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) - Switches between memory contexts
  - [shdepReassignOwned_Owner](shdepReassignOwned_Owner.md) - Handles ownership reassignment for OWNER dependencies
  - [shdepReassignOwned_InitAcl](shdepReassignOwned_InitAcl.md) - Handles initial ACL reassignment for INITACL dependencies
  - CommandCounterIncrement - Ensures changes are visible to subsequent operations

- Called from (representative examples):
  - [ReassignOwnedObjects](../R/ReassignOwnedObjects.md) (src/backend/commands/user.c:1641)

## Notes and Other Information
- Protected against reassigning ownership from pinned system roles
- Only processes objects in the current database or shared objects (dbid filtering)
- Uses memory context management to prevent memory leaks during bulk operations
- Calls CommandCounterIncrement after each object to ensure transaction visibility
- Part of the role management infrastructure, typically called during REASSIGN OWNED operations
- Does not modify grants or ACLs, only ownership relationships
- Delegates actual ownership changes to specialized helper functions for different dependency types