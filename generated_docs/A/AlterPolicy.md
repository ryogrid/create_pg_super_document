# AlterPolicy

## Location
[src/backend/commands/policy.c:768-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L768-L1095)

## Overview
Handles the execution of the ALTER POLICY command by modifying an existing row-level security policy's attributes including roles, USING clause, and WITH CHECK clause while maintaining proper dependency relationships.

## Definition


## Detailed Description
This function implements the ALTER POLICY SQL command through a comprehensive modification process:

1. **Policy Lookup**: Locates the existing policy by table OID and policy name, validating its existence
2. **Selective Updates**: Only modifies policy attributes that are explicitly specified in the ALTER statement (roles, USING clause, or WITH CHECK clause)
3. **Expression Processing**: For updated clauses, parses and transforms new expressions; for unchanged clauses, reconstructs dependencies from existing catalog data
4. **Command Validation**: Ensures clause combinations remain valid for the policy's command type (e.g., INSERT policies can't have USING clauses)
5. **Dependency Maintenance**: Completely recreates all dependency records to reflect the new policy state
6. **Atomic Updates**: Uses catalog tuple modification with proper locking to ensure consistency

The function handles partial updates efficiently by preserving unchanged attributes and only processing modified components.

## Parameters
- : AlterPolicyStmt structure containing modification details including:
  - Policy name and target table to identify the policy
  - Optional new roles list (NULL if unchanged)
  - Optional new USING clause expression (NULL if unchanged)
  - Optional new WITH CHECK clause expression (NULL if unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - [policy_role_list_to_array](../p/policy_role_list_to_array.md), construct_array_builtin (role processing)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md), relation_open (table access)
  - [make_parsestate](../m/make_parsestate.md), transformWhereClause, assign_expr_collations (expression parsing)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (policy lookup)
  - [heap_getattr](../h/heap_getattr.md), heap_modify_tuple, heap_freetuple (tuple manipulation)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md), deleteSharedDependencyRecordsFor (dependency cleanup)
  - [recordDependencyOn](../r/recordDependencyOn.md), recordDependencyOnExpr, recordSharedDependencyOn (dependency creation)
  - [stringToNode](../s/stringToNode.md), nodeToString (expression serialization)
  - InvokeObjectPostAlterHook, CacheInvalidateRelcache (hooks and cache management)
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main utility command dispatcher)

## Notes and Other Information
- Requires AccessExclusiveLock on target table to prevent concurrent operations
- Validates command-specific clause restrictions (same as CREATE POLICY)
- Handles NULL values for unchanged attributes by preserving existing catalog data
- Reconstructs range tables for unchanged expressions to maintain proper dependencies
- Does not create dependencies on the PUBLIC role
- Returns ObjectAddress of the modified policy for use by event and dependency systems
- Completely rebuilds all dependency records rather than incrementally updating them for simplicity and correctness