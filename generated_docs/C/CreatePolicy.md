# CreatePolicy

## Location
src/backend/commands/policy.c: 569 - 767

## Overview
Handles the execution of the CREATE POLICY command by creating a new row-level security policy in the pg_policy system catalog with specified access control rules and role assignments.

## Definition


## Detailed Description
This function implements the CREATE POLICY SQL command by performing comprehensive validation and catalog operations:

1. **Command Validation**: Validates policy command type (SELECT, INSERT, UPDATE, DELETE) and ensures proper clause combinations (e.g., WITH CHECK not allowed for SELECT/DELETE)
2. **Role Processing**: Converts the list of applicable roles into an array format suitable for catalog storage
3. **Expression Parsing**: Transforms USING and WITH CHECK clauses into internal expression trees using separate parse states for each
4. **Catalog Operations**: Creates a new entry in pg_policy with generated OID and validates uniqueness of policy names per table
5. **Dependency Management**: Records dependencies on the target table, referenced expressions, and applicable roles
6. **Security Integration**: Handles both permissive and restrictive policy types with proper expression collation assignment

The function ensures atomicity through catalog locking and maintains referential integrity through the dependency system.

## Parameters
- : CreatePolicyStmt structure containing all policy definition details including:
  - Policy name and target table
  - Command type (SELECT/INSERT/UPDATE/DELETE)
  - Applicable roles list
  - USING clause expression (qualification)
  - WITH CHECK clause expression (for INSERT/UPDATE)
  - Permissive/restrictive policy type

## Dependencies
- Functions called/Symbols referenced:
  - parse_policy_command (command type parsing)
  - policy_role_list_to_array, construct_array_builtin (role array construction)
  - make_parsestate, transformWhereClause (expression parsing)
  - RangeVarGetRelidExtended, relation_open (table access and permissions)
  - systable_beginscan, systable_getnext (catalog scanning)
  - CatalogTupleInsert, heap_form_tuple (catalog modifications)
  - recordDependencyOn, recordDependencyOnExpr, recordSharedDependencyOn (dependency tracking)
  - InvokeObjectPostCreateHook, CacheInvalidateRelcache (event hooks and cache management)
- Called from:
  - ProcessUtilitySlow (main utility command dispatcher)

## Notes and Other Information
- Requires AccessExclusiveLock on target table to prevent concurrent DDL operations
- Validates policy name uniqueness per table (duplicate names across different tables are allowed)
- INSERT policies can only have WITH CHECK clauses, not USING clauses
- SELECT and DELETE policies cannot have WITH CHECK clauses
- Does not create dependencies on the PUBLIC role as it's implicitly available
- Returns ObjectAddress of the created policy for use by dependency and event systems
- Supports both permissive (OR-ed) and restrictive (AND-ed) policy types introduced in PostgreSQL 10