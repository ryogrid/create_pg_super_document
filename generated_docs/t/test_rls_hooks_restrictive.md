# test_rls_hooks_restrictive

## Location
src/test/modules/test_rls_hooks/test_rls_hooks.c: 113 - 165

## Overview
Test hook function that generates restrictive Row Level Security (RLS) policies for specified test tables, implementing supervisor-based access control for testing restrictive RLS policy functionality.

## Definition


## Detailed Description
The  function is a test implementation of a Row Level Security restrictive policy hook. It dynamically creates RLS policies that restrict access to rows where the `supervisor` column matches the current PostgreSQL user. This function complements the permissive policy hook to test PostgreSQL's complete RLS hook infrastructure.

Restrictive policies in RLS work by denying access - all restrictive policies must allow access for a user to access a row. This means restrictive policies are combined with AND logic, while permissive policies use OR logic. The function creates a policy that compares the current user (from `current_user` function) with a `supervisor` column in the target table.

Like its permissive counterpart, this function only operates on specific test tables: `rls_test_restrictive` and `rls_test_both`. For other tables, it returns an empty policy list.

As noted in the source comments, for restrictive policies to be effective, at least one permissive policy must exist, otherwise the default-deny behavior will make everything invisible. The restrictive policy adds additional constraints on top of any permissive policies.

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
  - RLS policy hook system via `row_security_policy_hook_restrictive`
  - PostgreSQL query planner when processing RLS policies

## Notes and Other Information
- Located in: `src/test/modules/test_rls_hooks/test_rls_hooks.c:113-165`
- Part of PostgreSQL's test infrastructure for Row Level Security
- Creates policy named "extension policy" that applies to PUBLIC role
- Policy expression: `current_user = supervisor`
- Only activates for tables named "rls_test_restrictive" and "rls_test_both"
- Registered as a hook in `_PG_init` via `row_security_policy_hook_restrictive`
- Works in conjunction with `test_rls_hooks_permissive` for comprehensive RLS testing
- Restrictive policies require at least one permissive policy to be effective
- The policy applies to all command types (indicated by `polcmd = '*'`)
- Includes proper collation assignment for expression evaluation
- Test module design allows verification of restrictive policy logic without affecting production systems
- Important for testing the AND logic behavior of restrictive policies versus the OR logic of permissive policies