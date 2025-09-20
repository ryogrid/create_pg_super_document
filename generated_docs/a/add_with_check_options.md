# add_with_check_options

## Location
[src/backend/rewrite/rowsecurity.c:796-807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rowsecurity.c#L796-L807)

## Overview
Adds WithCheckOptions of a specified kind to ensure that new records from INSERT or UPDATE operations comply with Row Level Security (RLS) policies.

## Definition

```c
static void
add_with_check_options(Relation rel,
					   int rt_index,
					   WCOKind kind,
					   List *permissive_policies,
					   List *restrictive_policies,
					   List **withCheckOptions,
					   bool *hasSubLinks,
					   bool force_using)
```
## Detailed Description
This function creates WithCheckOption nodes that enforce Row Level Security policies during data modification operations. It processes both permissive and restrictive RLS policies to generate appropriate checks:

- For permissive policies, it combines all policy clauses using OR logic into a single WithCheckOption
- For restrictive policies, it creates separate WithCheckOptions for each policy (combined with AND logic)
- Policies can use either explicit WITH CHECK clauses or fall back to USING clauses when  is true or no WITH CHECK clause exists
- In cases where no permissive policies grant access, it creates a default-deny WithCheckOption that always fails
- Special handling exists for INSERT ... ON CONFLICT DO UPDATE scenarios using WCO_RLS_CONFLICT_CHECK

## Parameters / Member Variables
- : The relation (table) for which WITH CHECK options are being added
- : Range table index of the relation in the query
- : Type of WithCheckOption to create (WCO_RLS_INSERT_CHECK, WCO_RLS_UPDATE_CHECK, etc.)
- : List of permissive RLS policies to process
- : List of restrictive RLS policies to process  
- : Output parameter - list to append new WithCheckOptions to
- : Output parameter - set to true if any policy contains sublink subqueries
- : When true, forces use of USING clauses instead of WITH CHECK clauses

## Dependencies
- Functions called/Symbols referenced:
  - [WCOKind](../W/WCOKind.md) (enum type)
  - makeNode
  - RelationGetRelationName
  - [pstrdup](../p/pstrdup.md)
  - lappend
  - copyObject
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [list_append_unique](../l/list_append_unique.md)
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - [makeConst](../m/makeConst.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
- Called from (representative examples):
  - [get_row_security_policies](../g/get_row_security_policies.md) (multiple call sites for different WCO kinds)

## Notes and Other Information
- This is a static function within src/backend/rewrite/rowsecurity.c:796-907
- Uses a macro QUAL_FOR_WCO to determine whether to use WITH CHECK or USING clauses
- Handles the security model where permissive policies grant access (OR logic) while restrictive policies add additional constraints (AND logic)
- Critical for enforcing data security in PostgreSQL's Row Level Security feature
- [Variable](../V/Variable.md) nodes are adjusted using ChangeVarNodes to reference the correct range table entry