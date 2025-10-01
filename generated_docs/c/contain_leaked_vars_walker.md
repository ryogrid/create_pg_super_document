# contain_leaked_vars_walker

## Location
[src/backend/optimizer/util/clauses.c:1275-1455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1275-L1455)

## Overview
The `contain_leaked_vars_walker` function is a tree-walking function that recursively traverses expression nodes to detect whether any Var nodes are passed to non-leakproof functions that could potentially leak sensitive data.

## Definition
```c
static bool contain_leaked_vars_walker(Node *node, void *context)
```

## Detailed Description
This function implements the core logic for detecting potentially leaky expressions in PostgreSQL queries. It performs a depth-first traversal of expression trees, analyzing each node type to determine if it could compromise data security through non-leakproof function calls.

The function uses a comprehensive switch statement to handle different node types appropriately:

- **Safe nodes** (T_Var, T_Const, etc.): Treated as inherently safe but continue traversal to check children
- **Function-calling nodes** (T_FuncExpr, T_OpExpr, etc.): Check if functions are leakproof and if they operate on variables
- **Special cases**: SubscriptingRef, RowCompareExpr, and MinMaxExpr receive specialized handling
- **Unknown nodes**: Conservatively treated as potentially leaky for security

The function is recursive and uses `expression_tree_walker` to continue traversal when the current node doesn't immediately trigger a leak detection.

## Parameters / Member Variables
- `node`: A Node pointer representing the current expression node being analyzed
- `context`: A void pointer for additional context information (passed through but not used directly in this function)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - [check_functions_in_node](check_functions_in_node.md)
  - [contain_leaked_vars_checker](contain_leaked_vars_checker.md)
  - [contain_var_clause](contain_var_clause.md)
  - [getSubscriptingRoutines](../g/getSubscriptingRoutines.md)
  - [get_opcode](../g/get_opcode.md)
  - [get_func_leakproof](../g/get_func_leakproof.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - expression_tree_walker
- Called from (representative examples):
  - [contain_leaked_vars](contain_leaked_vars.md)
  - max_parallel_hazard_context (self-recursively)

## Notes and Other Information
- Returns true if any non-leakproof function is found that operates on variables
- Static function used internally within the clauses.c module
- Handles complex expression types including row comparisons, min/max expressions, and array subscripting
- Uses conservative approach: unknown node types are assumed to be potentially leaky
- [CurrentOfExpr](../C/CurrentOfExpr.md) is explicitly treated as non-leaky since TID scans must always be generated
- Part of PostgreSQL's comprehensive security infrastructure for preventing data leakage
- Located in src/backend/optimizer/util/clauses.c:1275-1455

## Simplified Source

```c
static bool
contain_leaked_vars_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    switch (nodeTag(node))
    {
        // Safe node types - check children
        case T_Var:
        case T_Const:
        case T_Param:
        case T_BoolExpr:
        case T_CaseExpr:
        case T_List:
            // These don't contain function calls
            break;

        // Function-calling nodes - check if leaky
        case T_FuncExpr:
        case T_OpExpr:
        case T_DistinctExpr:
        case T_ScalarArrayOpExpr:
            // If node contains leaky function AND has Vars, reject
            if (check_functions_in_node(node, contain_leaked_vars_checker,
                                      context) &&
                contain_var_clause(node))
                return true;
            break;

        case T_SubscriptingRef:
            {
                SubscriptingRef *sbsref = (SubscriptingRef *) node;
                const SubscriptRoutines *sbsroutines;

                // Check if subscripting operations are leakproof
                sbsroutines = getSubscriptingRoutines(sbsref->refcontainertype,
                                                    NULL);
                if (!sbsroutines ||
                    !(sbsref->refassgnexpr != NULL ?
                      sbsroutines->store_leakproof :
                      sbsroutines->fetch_leakproof))
                {
                    if (contain_var_clause(node))
                        return true;
                }
            }
            break;

        case T_RowCompareExpr:
            {
                // Check each comparison operator for leakproof
                RowCompareExpr *rcexpr = (RowCompareExpr *) node;
                ListCell *opid, *larg, *rarg;

                forthree(opid, rcexpr->opnos,
                        larg, rcexpr->largs,
                        rarg, rcexpr->rargs)
                {
                    Oid funcid = get_opcode(lfirst_oid(opid));

                    if (!get_func_leakproof(funcid) &&
                        (contain_var_clause((Node *) lfirst(larg)) ||
                         contain_var_clause((Node *) lfirst(rarg))))
                        return true;
                }
            }
            break;

        case T_CurrentOfExpr:
            // Always safe - TID scans must be generated
            return false;

        default:
            // Unknown node types assumed potentially leaky
            return true;
    }

    return expression_tree_walker(node, contain_leaked_vars_walker, context);
}
```