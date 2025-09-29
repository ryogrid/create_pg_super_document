# expr_setup_walker

## Location
[src/backend/executor/execExpr.c:2828-2884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L2828-L2884)

## Overview
A specialized expression tree walker that analyzes expressions to collect setup requirements, tracking variable access patterns and identifying MULTIEXPR subplans.

## Definition

```c
static bool
expr_setup_walker(Node *node, ExprSetupInfo *info)
```

## Detailed Description
expr_setup_walker is a recursive tree traversal function that implements the analysis phase of PostgreSQL's expression setup system. It systematically examines an expression tree to identify components that require special setup steps before the main expression evaluation can occur.

The walker performs several key analyses:

1. **Variable access tracking**: For each Var node encountered, it records the highest attribute number accessed for each slot type (inner, outer, scan). This information is used to determine the minimum amount of tuple deformation needed.

2. **MULTIEXPR subplan collection**: Identifies and collects SubPlan nodes with MULTIEXPR_SUBLINK type, which require special handling during setup because they must be evaluated before their output parameters are referenced.

3. **Selective traversal**: Implements special handling for aggregate functions (Aggref), window functions (WindowFunc), and grouping functions (GroupingFunc) by not examining their arguments, since those arguments are evaluated in different contexts.

The function uses PostgreSQL's standard expression_tree_walker infrastructure for recursive traversal, ensuring consistent and complete tree examination while allowing for specialized handling of specific node types.

## Parameters / Member Variables
- node: The current expression node being examined during tree traversal
- info: ExprSetupInfo structure that accumulates setup requirements found during analysis

## Dependencies
- Functions called/Symbols referenced:
  - expression_tree_walker (standard PostgreSQL expression tree traversal infrastructure)
  - [lappend](../l/lappend.md) (list manipulation for collecting MULTIEXPR subplans)
  - Max (macro for tracking maximum attribute numbers)
  - IsA (node type checking macros)
- Called from (representative examples):
  - [ExecCreateExprSetupSteps](../E/ExecCreateExprSetupSteps.md) (primary expression analysis entry point)
  - [ExecBuildUpdateProjection](../E/ExecBuildUpdateProjection.md) (update projection analysis)
  - [ExecBuildAggTrans](../E/ExecBuildAggTrans.md) (aggregate transition expression analysis)
  - [expr_setup_walker](expr_setup_walker.md) (recursive self-calls during tree traversal)

## Notes and Other Information
- Returns false from Var nodes to prevent unnecessary recursion into leaf nodes
- Uses varno field to distinguish between INNER_VAR, OUTER_VAR, and scan variables (default case includes INDEX_VAR)
- The function specifically avoids examining arguments of aggregate and window functions because those are evaluated in separate execution contexts
- MULTIEXPR subplans are collected in a list for later processing during setup step generation
- This walker is part of a two-phase approach: first analyze (this function), then generate steps (ExecPushExprSetupSteps)
- The function is static and only used internally within the expression evaluation system

## Simplified Source

```c
static bool
expr_setup_walker(Node *node, ExprSetupInfo *info)
{
    // Base case: null node
    if (node == NULL)
        return false;

    // Track variable access patterns for slot deformation
    if (IsA(node, Var))
    {
        Var *variable = (Var *) node;
        AttrNumber attnum = variable->varattno;

        // Update maximum attribute needed for each slot type
        switch (variable->varno)
        {
            case INNER_VAR:
                info->last_inner = Max(info->last_inner, attnum);
                break;
            case OUTER_VAR:
                info->last_outer = Max(info->last_outer, attnum);
                break;
            default:
                // INDEX_VAR handled here too
                info->last_scan = Max(info->last_scan, attnum);
                break;
        }
        return false; // No need to recurse into Var nodes
    }

    // Collect MULTIEXPR SubPlans for special handling
    if (IsA(node, SubPlan))
    {
        SubPlan *subplan = (SubPlan *) node;
        if (subplan->subLinkType == MULTIEXPR_SUBLINK)
            info->multiexpr_subplans = lappend(info->multiexpr_subplans, subplan);
    }

    // Skip function arguments that execute in different contexts
    if (IsA(node, Aggref) || IsA(node, WindowFunc) || IsA(node, GroupingFunc))
        return false;

    // Continue walking the expression tree
    return expression_tree_walker(node, expr_setup_walker, (void *) info);
}
```