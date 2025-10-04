# test_rls_hooks_restrictive

## Location
[src/test/modules/test_rls_hooks/test_rls_hooks.c:113-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rls_hooks/test_rls_hooks.c#L113-L165)

## Overview
Test hook function that generates restrictive Row Level Security (RLS) policies for specified test tables, implementing supervisor-based access control for testing restrictive RLS policy functionality.

## Definition

```c
struct_array_builtin(&role, 1, OIDOID);
```
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
  - [make_parsestate](../m/make_parsestate.md) (create parser state)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md) (add table to parse state)
  - [addNSItemToQuery](../a/addNSItemToQuery.md) (add namespace item)
  - [palloc0](../p/palloc0.md) (allocate zeroed memory)
  - [pstrdup](../p/pstrdup.md) (duplicate string)
  - [construct_array_builtin](../c/construct_array_builtin.md) (create array)
  - `[makeFuncCall](../m/makeFuncCall.md)` (create function call node)
  - `makeNode` (create AST node)
  - `[makeSimpleA_Expr](../m/makeSimpleA_Expr.md)` (create expression node)
  - [transformWhereClause](transformWhereClause.md) (transform WHERE clause)
  - [assign_expr_collations](../a/assign_expr_collations.md) (assign collation info)
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

## Simplified Source

```c
List *
test_rls_hooks_restrictive(CmdType cmdtype, Relation relation)
{
    // Only process specific test tables
    if (strcmp(RelationGetRelationName(relation), "rls_test_restrictive") != 0 &&
        strcmp(RelationGetRelationName(relation), "rls_test_both") != 0)
        return NIL;

    // Create policy structure
    RowSecurityPolicy *policy = palloc0(sizeof(RowSecurityPolicy));
    ParseState *qual_pstate = make_parsestate(NULL);

    // Set up table access for parsing
    ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(qual_pstate,
                                                              relation, AccessShareLock,
                                                              NULL, false, false);
    addNSItemToQuery(qual_pstate, nsitem, false, true, true);

    // Configure basic policy properties
    policy->policy_name = pstrdup("extension policy");
    policy->polcmd = '*';  // Apply to all command types

    // Set policy to apply to PUBLIC role
    Datum role = ObjectIdGetDatum(ACL_ID_PUBLIC);
    policy->roles = construct_array_builtin(&role, 1, OIDOID);

    // Build restrictive policy expression: current_user = supervisor
    FuncCall *current_user_func = makeFuncCall(list_make2(makeString("pg_catalog"),
                                                         makeString("current_user")),
                                              NIL, COERCE_EXPLICIT_CALL, -1);

    ColumnRef *supervisor_col = makeNode(ColumnRef);
    supervisor_col->fields = list_make1(makeString("supervisor"));
    supervisor_col->location = 0;

    // Create equality expression
    Node *equality_expr = (Node *) makeSimpleA_Expr(AEXPR_OP, "=",
                                                    (Node *) current_user_func,
                                                    (Node *) supervisor_col, 0);

    // Transform and assign policy qualifications
    policy->qual = (Expr *) transformWhereClause(qual_pstate, copyObject(equality_expr),
                                                 EXPR_KIND_POLICY, "POLICY");
    assign_expr_collations(qual_pstate, (Node *) policy->qual);

    policy->with_check_qual = copyObject(policy->qual);
    policy->hassublinks = false;

    return list_make1(policy);
}
```