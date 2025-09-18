# rename_constraint_internal

## Location
[src/backend/commands/tablecmds.c:3915-4020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3915-L4020)

## Overview
rename_constraint_internal is the core internal function that performs constraint renaming operations for both table and domain constraints, handling inheritance hierarchies and constraint type-specific logic.

## Definition
static ObjectAddress rename_constraint_internal(Oid myrelid, Oid mytypid, const char *oldconname, const char *newconname, bool recurse, bool recursing, int expected_parents)

## Detailed Description
rename_constraint_internal implements the logic for renaming constraints on both relations and domains. The function handles different constraint types appropriately - for indexed constraints (PRIMARY KEY, UNIQUE, EXCLUSION), it renames the underlying index which automatically renames the constraint. For other constraint types, it directly calls RenameConstraintById.

The function includes comprehensive inheritance handling for CHECK constraints, recursively renaming constraints in child tables when requested, or enforcing that inheritance hierarchies are handled correctly when recursion is disabled. It performs validation through renameatt_check for relation constraints and manages cache invalidation to ensure consistency.

## Parameters / Member Variables
- `myrelid`: OID of the relation containing the constraint (0 if domain constraint)
- `mytypid`: OID of the domain type (0 if relation constraint)  
- `oldconname`: Current name of the constraint to rename
- `newconname`: New name for the constraint
- `recurse`: Whether to recursively rename constraints in child tables
- `recursing`: Whether this call is part of a recursive operation
- `expected_parents`: Expected number of parent tables inheriting this constraint

## Dependencies
- Functions called/Symbols referenced:
  - [get_domain_constraint_oid](../g/get_domain_constraint_oid.md)
  - [get_relation_constraint_oid](../g/get_relation_constraint_oid.md)
  - [relation_open](relation_open.md)
  - [renameatt_check](renameatt_check.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [RenameConstraintById](../R/RenameConstraintById.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [relation_close](relation_close.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [RenameConstraint](../R/RenameConstraint.md) (in src/backend/commands/tablecmds.c)
  - [rename_constraint_internal](rename_constraint_internal.md) (recursive calls)

## Notes and Other Information
- Uses AccessExclusiveLock when opening relations to prevent concurrent modifications
- Handles both relation constraints and domain constraints through separate code paths
- For indexed constraints, renames the underlying index rather than the constraint directly
- Validates inheritance relationships and prevents incorrect constraint renaming in inheritance hierarchies
- Performs cache invalidation to ensure other sessions see the constraint name change
- Similar logic structure to renameatt_internal for consistency
- Supports both recursive and non-recursive constraint renaming operations