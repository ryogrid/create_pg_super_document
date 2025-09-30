# cost_qual_eval_walker

## Location
[src/backend/optimizer/path/costsize.c:4683-4964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L4683-L4964)

## Overview
A recursive tree-walking function that computes detailed execution costs for individual expression nodes in qualification clauses.

## Definition

```c
struct equivalence to treat these all alike */
		set_opfuncid((OpExpr *) node);
```
## Detailed Description
The  function is the core workhorse of PostgreSQL's qualification cost estimation system. It performs a depth-first traversal of expression trees, accumulating execution costs for each node type based on their computational complexity and execution characteristics.

The function implements sophisticated cost modeling for various expression types:

**RestrictInfo Caching**: Uses eval_cost field to cache computed costs, avoiding redundant calculations for the same expressions. Handles OR clauses and pseudoconstant expressions specially.

**Function/Operator Costs**: Charges actual execution costs from pg_proc.procost for functions and operators, multiplied by cpu_operator_cost. Handles complex cases like ScalarArrayOpExpr with both hashed and linear search strategies.

**Special Node Types**: Provides specialized handling for aggregates (zero cost as they're handled at plan node level), type coercion, array operations, row comparisons, and subplans.

**Expression Tree Traversal**: Uses expression_tree_walker for recursive traversal while implementing custom logic for nodes that shouldn't recurse (like aggregates, subplans, and placeholders).

The function distinguishes between startup costs (paid once) and per-tuple costs (paid for each row), enabling accurate cost modeling for different execution contexts.

## Parameters / Member Variables
- : Expression node being evaluated for cost estimation
- : Cost evaluation context containing accumulated costs and planner information

## Dependencies
- Functions called/Symbols referenced:
  - [add_function_cost](../a/add_function_cost.md)
  - [set_opfuncid](../s/set_opfuncid.md)
  - [set_sa_opfuncid](../s/set_sa_opfuncid.md)
  - [estimate_array_length](../e/estimate_array_length.md)
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [get_opcode](../g/get_opcode.md)
  - expression_tree_walker
  - cost_qual_eval_context (struct)
  - Various expression node types (FuncExpr, OpExpr, ScalarArrayOpExpr, etc.)
- Called from (representative examples):
  - [cost_qual_eval](cost_qual_eval.md)
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [cost_qual_eval_walker](cost_qual_eval_walker.md) (recursive calls)

## Notes and Other Information
- Static function within costsize.c, used internally by the cost estimation system
- Implements caching through RestrictInfo.eval_cost to avoid redundant calculations
- Does not account for short-circuit evaluation of AND/OR to maintain clause ordering independence
- Ignores set-returning function multiplication effects due to complexity and rarity
- Handles various PostgreSQL-specific node types including subplans, coercion, and JSON expressions
- Critical for accurate cost-based optimization across all plan types that evaluate expressions
- Returns false to prevent recursion for nodes with special handling (aggregates, subplans, placeholders)

## Simplified Source

```c
static bool cost_qual_eval_walker(Node *node, cost_qual_eval_context *context) {
    if (node == NULL)
        return false;

    // Handle RestrictInfo nodes with caching
    if (IsA(node, RestrictInfo)) {
        RestrictInfo *rinfo = (RestrictInfo *) node;

        // Check if cost already computed (cached)
        if (rinfo->eval_cost.startup < 0) {
            // Compute cost for this RestrictInfo
            cost_qual_eval_context locContext = {0};
            locContext.root = context->root;

            // Recurse into clause or OR clause
            if (rinfo->orclause)
                cost_qual_eval_walker((Node *) rinfo->orclause, &locContext);
            else
                cost_qual_eval_walker((Node *) rinfo->clause, &locContext);

            // Handle pseudoconstant: convert per-tuple to startup cost
            if (rinfo->pseudoconstant) {
                locContext.total.startup += locContext.total.per_tuple;
                locContext.total.per_tuple = 0;
            }
            rinfo->eval_cost = locContext.total;
        }

        // Add cached cost to context
        context->total.startup += rinfo->eval_cost.startup;
        context->total.per_tuple += rinfo->eval_cost.per_tuple;
        return false;  // Don't recurse further
    }

    // Handle function calls
    if (IsA(node, FuncExpr)) {
        add_function_cost(context->root, ((FuncExpr *) node)->funcid, node, &context->total);
    }

    // Handle operators (OpExpr, DistinctExpr, NullIfExpr)
    else if (IsA(node, OpExpr) || IsA(node, DistinctExpr) || IsA(node, NullIfExpr)) {
        set_opfuncid((OpExpr *) node);
        add_function_cost(context->root, ((OpExpr *) node)->opfuncid, node, &context->total);
    }

    // Handle scalar array operations (IN/NOT IN with arrays)
    else if (IsA(node, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) node;
        Node *arraynode = (Node *) lsecond(saop->args);
        double estarraylen = estimate_array_length(context->root, arraynode);

        set_sa_opfuncid(saop);

        if (OidIsValid(saop->hashfuncid)) {
            // Hashed array operation
            QualCost sacosts = {0}, hcosts = {0};
            add_function_cost(context->root, saop->opfuncid, NULL, &sacosts);
            add_function_cost(context->root, saop->hashfuncid, NULL, &hcosts);

            context->total.startup += sacosts.startup + hcosts.startup;
            context->total.startup += estarraylen * hcosts.per_tuple;  // Build hash table
            context->total.per_tuple += hcosts.per_tuple + sacosts.per_tuple;  // Lookup
        } else {
            // Linear search through array (check ~half elements on average)
            QualCost sacosts = {0};
            add_function_cost(context->root, saop->opfuncid, NULL, &sacosts);
            context->total.startup += sacosts.startup;
            context->total.per_tuple += sacosts.per_tuple * estarraylen * 0.5;
        }
    }

    // Handle aggregates and window functions (zero cost - handled at plan level)
    else if (IsA(node, Aggref) || IsA(node, WindowFunc)) {
        return false;  // Don't recurse
    }

    // Handle type coercion via I/O
    else if (IsA(node, CoerceViaIO)) {
        CoerceViaIO *iocoerce = (CoerceViaIO *) node;
        Oid iofunc, typioparam;
        bool typisvarlena;

        // Cost both input and output functions
        getTypeInputInfo(iocoerce->resulttype, &iofunc, &typioparam);
        add_function_cost(context->root, iofunc, NULL, &context->total);
        getTypeOutputInfo(exprType((Node *) iocoerce->arg), &iofunc, &typisvarlena);
        add_function_cost(context->root, iofunc, NULL, &context->total);
    }

    // Handle subplans (executed per evaluation)
    else if (IsA(node, SubPlan)) {
        SubPlan *subplan = (SubPlan *) node;
        context->total.startup += subplan->startup_cost;
        context->total.per_tuple += subplan->per_call_cost;
        return false;  // Don't recurse into testexpr
    }

    // Handle simple expression types with fixed cost
    else if (IsA(node, GroupingFunc) || IsA(node, MinMaxExpr) ||
             IsA(node, SQLValueFunction) || IsA(node, XmlExpr) ||
             IsA(node, CoerceToDomain) || IsA(node, NextValueExpr) ||
             IsA(node, JsonExpr)) {
        context->total.per_tuple += cpu_operator_cost;
    }

    // Handle current-of expressions (expensive to discourage non-TID scans)
    else if (IsA(node, CurrentOfExpr)) {
        context->total.startup += disable_cost;
    }

    // Handle placeholders (zero cost - computed elsewhere)
    else if (IsA(node, PlaceHolderVar)) {
        return false;  // Don't recurse
    }

    // Continue recursion for other node types
    return expression_tree_walker(node, cost_qual_eval_walker, (void *) context);
}
```