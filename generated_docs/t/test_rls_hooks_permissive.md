# test_rls_hooks_permissive

## Location
src/test/modules/test_rls_hooks/test_rls_hooks.c: 45 - 112

## Overview
Test hook function that generates permissive Row Level Security (RLS) policies for specified test tables, implementing username-based access control for testing RLS hook functionality.

## Definition


## Detailed Description
The  function is a test implementation of a Row Level Security permissive policy hook. It dynamically creates RLS policies that allow users to access rows where the `username` column matches the current PostgreSQL user. This function is specifically designed for testing PostgreSQL's RLS hook infrastructure.

Permissive policies in RLS work by granting access - if any permissive policy allows access to a row, the user can access it. This function creates a policy that compares the current user (from `current_user` function) with a `username` column in the target table.

The function only operates on two specific test tables: `rls_test_permissive` and `rls_test_both`. For other tables, it returns an empty policy list.

The generated policy includes both qualification and with-check qualification expressions, ensuring consistent access control for both reading and writing operations.

## Parameters / Member Variables
- `cmdtype`: The type of SQL command being executed (SELECT, INSERT, UPDATE, DELETE)
- `relation`: Pointer to the relation (table) structure for which policies are being requested

## Dependencies
- Functions called/Symbols referenced:
  - `RelationGetRelationName` (get table name)
  - `make_parsestate` (create parser state)
  - `addRangeTableEntryForRelation` (add table to parse state)
  - `addNSItemToQuery` (add namespace item)
  - `palloc0` (allocate zeroed memory)
  - `pstrdup` (duplicate string)
  - `construct_array_builtin` (create array)
  - `makeFuncCall` (create function call node)
  - `makeNode` (create AST node)
  - `makeSimpleA_Expr` (create expression node)
  - `transformWhereClause` (transform WHERE clause)
  - `assign_expr_collations` (assign collation info)
  - `copyObject` (deep copy objects)
  - `list_make1` (create single-element list)

- Called from (representative examples):
  - RLS policy hook system via `row_security_policy_hook_permissive`
  - PostgreSQL query planner when processing RLS policies

## Notes and Other Information
- Located in: `src/test/modules/test_rls_hooks/test_rls_hooks.c:45-102`
- Part of PostgreSQL's test infrastructure for Row Level Security
- Creates policy named "extension policy" that applies to PUBLIC role
- Policy expression: `current_user = username` 
- Only activates for tables named "rls_test_permissive" and "rls_test_both"
- Registered as a hook in `_PG_init` via `row_security_policy_hook_permissive`
- Used in conjunction with `test_rls_hooks_restrictive` for comprehensive RLS testing
- The policy applies to all command types (indicated by `polcmd = '*'`)
- Includes proper collation assignment for expression evaluation
- Test module design allows verification of RLS hook integration without affecting production code