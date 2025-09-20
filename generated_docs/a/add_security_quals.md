# add_security_quals

## Location
[src/backend/rewrite/rowsecurity.c:700-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rowsecurity.c#L700-L795)

## Overview
This static function constructs and adds security qualifier expressions that enforce row-level security policies during query execution to restrict access to existing table data.

## Definition

```c
static void
add_security_quals(int rt_index,
				   List *permissive_policies,
				   List *restrictive_policies,
				   List **securityQuals,
				   bool *hasSubLinks)
```
## Detailed Description
The  function is responsible for converting row-level security policies into executable SQL qualifiers that filter rows during query execution. It implements PostgreSQL's dual-policy model where:

- **Permissive policies** are combined using OR logic - if any permissive policy allows access, the row is visible
- **Restrictive policies** are combined using AND logic - all restrictive policies must allow access

The function handles the critical security principle that if no permissive policies exist, no rows should be visible (default-deny). It processes policies by copying and adjusting their USING clauses, converting table-relative column references to query-specific references using .

When combining multiple permissive policy qualifiers, it creates a single OR expression. Restrictive qualifiers are added individually since they must all be satisfied. If no permissive policies are found, the function adds an always-false constant to ensure no rows are visible.

## Parameters / Member Variables
- : Range table index for the relation being secured
- : List of permissive policies to be applied with OR logic
- : List of restrictive policies to be applied with AND logic
- : Output parameter - list of security qualifier expressions to add to the query
- : Output parameter - set to true if any policy contains subquery expressions

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [list_append_unique](../l/list_append_unique.md)
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - [makeConst](../m/makeConst.md)
  - linitial/list_length
- Called from (representative examples):
  - [get_row_security_policies](../g/get_row_security_policies.md) (for SELECT, UPDATE, DELETE operations)

## Notes and Other Information
- Implements PostgreSQL's default-deny security model - no permissive policies means no access
- Uses  to adjust column references from policy context to query context
- Restrictive policies are added individually to ensure all constraints are enforced
- Permissive policies are combined into a single OR expression for efficiency
- Tracks sublink presence to inform query planning decisions
- The function ensures policy qualifiers are unique using 
- Policy expressions are deep-copied to avoid interference between different query contexts