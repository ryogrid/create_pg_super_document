# LockTableRecurse

## Location
src/backend/commands/lockcmds.c: 117 - 173

## Overview
Recursively applies table locks across an inheritance hierarchy, locking all child tables that inherit from the specified parent table.

## Definition


## Detailed Description
LockTableRecurse implements the inheritance-aware locking mechanism for LOCK TABLE commands. When a table with child tables is locked, this function ensures all children in the inheritance tree receive the same lock mode. The function uses find_all_inheritors to discover the complete inheritance hierarchy, then iterates through each child table to acquire the requested lock. It handles both blocking and non-blocking (NOWAIT) lock modes, with appropriate error handling for concurrent table drops and lock conflicts. The function assumes permission checking has already been performed on the parent table, which is sufficient for child table access.

## Parameters / Member Variables
- : OID of the parent relation whose inheritance tree should be locked
- : The lock mode to apply to all tables in the inheritance hierarchy
- : Boolean flag indicating whether to use conditional (non-blocking) lock acquisition

## Dependencies
- Functions called/Symbols referenced:
  - find_all_inheritors
  - LockRelationOid
  - ConditionalLockRelationOid
  - get_rel_name
  - SearchSysCacheExists1
  - UnlockRelationOid
- Called from (representative examples):
  - LockTableCommand
  - LockViewRecurse_walker

## Notes and Other Information
- This is a static function, only accessible within the lockcmds.c module
- No permission checking is performed on child tables since parent table permissions are considered sufficient
- The function gracefully handles concurrent table drops by checking existence in the system catalog
- When using NOWAIT mode, lock failures result in informative error messages using relation names
- Useless locks on concurrently dropped tables are properly released to avoid resource leaks
- The parent table is skipped in the iteration since it should already be locked by the caller
- The function maintains transactional consistency by verifying table existence after lock acquisition