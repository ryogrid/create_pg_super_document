# shdepReassignOwned_InitAcl

## Location
src/backend/catalog/pg_shdepend.c: 1734 - 1759

## Overview
A static helper function that handles reassignment of role references in pg_init_privs entries during ownership reassignment operations for SHARED_DEPENDENCY_INITACL dependencies.

## Definition
```c
static void shdepReassignOwned_InitAcl(Form_pg_shdepend sdepForm, Oid oldrole, Oid newrole)
```

## Detailed Description
This function handles the reassignment of role references in PostgreSQL's pg_init_privs catalog during REASSIGN OWNED operations. The pg_init_privs table stores initial privileges that were set during CREATE EXTENSION operations.

The function replaces references to the old role with the new role in pg_init_privs entries. The implementation includes extensive commentary about design tradeoffs:

1. **Historical vs. Current State**: Ideally pg_init_privs should preserve historical state from extension creation time, but practical considerations necessitate updating role references
2. **DROP OWNED Consistency**: If old role references weren't updated, handling subsequent DROP OWNED operations would be problematic  
3. **pg_dump Compatibility**: Tools like pg_dump expect pg_init_privs entries to be consistent with current object ownership

The current approach prioritizes system consistency and tool compatibility over strict historical preservation.

## Parameters / Member Variables
- `sdepForm`: Pointer to pg_shdepend tuple containing dependency information (classid, objid, objsubid)
- `oldrole`: OID of the role being replaced in the initial privileges
- `newrole`: OID of the role that will replace the old role in initial privileges

## Dependencies
- Functions called/Symbols referenced:
  - [ReplaceRoleInInitPriv](../R/ReplaceRoleInInitPriv.md) - Updates role references in pg_init_privs entries for the specified object

- Called from (representative examples):
  - [shdepReassignOwned](shdepReassignOwned.md) (src/backend/catalog/pg_shdepend.c:1614)
  - ShDependObjectInfo (src/backend/catalog/pg_shdepend.c:107)

## Notes and Other Information
- Static function only used within pg_shdepend.c
- Part of the ownership reassignment infrastructure for REASSIGN OWNED operations
- Handles a specific design tradeoff between historical accuracy and practical system consistency
- The extensive comments document known limitations and design decisions
- Works in conjunction with shdepReassignOwned_Owner to provide complete ownership reassignment
- Ensures pg_init_privs entries remain consistent with current object ownership after reassignment