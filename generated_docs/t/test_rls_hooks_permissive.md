# test_rls_hooks_permissive

## Location
[src/test/modules/test_rls_hooks/test_rls_hooks.c:45-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rls_hooks/test_rls_hooks.c#L45-L112)

## Overview
Test hook function that generates permissive Row Level Security (RLS) policies for specified test tables, implementing username-based access control for testing RLS hook functionality.

## Definition

```c
struct_array_builtin(&role, 1, OIDOID);
```
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

## Simplified Source

```c
List *
test_rls_hooks_permissive(CmdType cmdtype, Relation relation)
{
    // Only process specific test tables
    if (strcmp(RelationGetRelationName(relation), "rls_test_permissive") != 0 &&
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

    // Build policy expression: current_user = username
    FuncCall *current_user_func = makeFuncCall(list_make2(makeString("pg_catalog"),
                                                         makeString("current_user")),
                                              NIL, COERCE_EXPLICIT_CALL, -1);

    ColumnRef *username_col = makeNode(ColumnRef);
    username_col->fields = list_make1(makeString("username"));
    username_col->location = 0;

    // Create equality expression
    Node *equality_expr = (Node *) makeSimpleA_Expr(AEXPR_OP, "=",
                                                    (Node *) current_user_func,
                                                    (Node *) username_col, 0);

    // Transform and assign policy qualifications
    policy->qual = (Expr *) transformWhereClause(qual_pstate, copyObject(equality_expr),
                                                 EXPR_KIND_POLICY, "POLICY");
    assign_expr_collations(qual_pstate, (Node *) policy->qual);

    policy->with_check_qual = copyObject(policy->qual);
    policy->hassublinks = false;

    return list_make1(policy);
}
```