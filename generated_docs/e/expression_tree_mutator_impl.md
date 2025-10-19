# expression_tree_mutator_impl

## Location
[src/backend/nodes/nodeFuncs.c:2933-2941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L2933-L2941)

## Overview
This function creates modified copies of expression trees, allowing nodes to be added, removed, or replaced while preserving the original tree structure through recursive copying.

## Definition
```c
Node *expression_tree_mutator_impl(Node *node,
                                  tree_mutator_callback mutator,
                                  void *context)
```

## Detailed Description
The `expression_tree_mutator_impl` function is the core implementation for creating modified copies of PostgreSQL expression trees. It handles the mechanical aspects of node copying while delegating specific transformation logic to user-provided mutator callbacks. The function supports a comprehensive set of PostgreSQL node types including expressions, operators, functions, aggregates, window functions, and various other SQL constructs.

The function works by examining each node's type and creating a shallow copy (FLATCOPY) of the node structure, then recursively calling the mutator function on each subnode that contains expressions. This ensures that transformations can be applied at any level of the expression tree while maintaining proper memory management and node relationships.

The function includes extensive documentation explaining the proper usage pattern: mutator functions should handle special cases for specific node types and then call `expression_tree_mutator` for standard recursive processing. This design allows mutators to have full control over the transformation process while avoiding the complexity of handling all possible node types.

## Parameters / Member Variables
- `node`: Pointer to the Node to be mutated/copied
- `mutator`: Callback function of type tree_mutator_callback that performs specific transformations
- `context`: User-defined context data passed to mutator callbacks

## Dependencies
- Functions called/Symbols referenced:
  - FLATCOPY (macro for shallow node copying)
  - MUTATE (macro for calling mutator on subnodes)
  - copyObject (for simple node types)
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - nodeTag (node type identification)
  - Various PostgreSQL node types (Var, Const, Param, Aggref, FuncExpr, etc.)
- Called from (representative examples):
  - expression_tree_mutator (inline wrapper)
  - planstate_tree_walker
  - Various query transformation functions

## Notes and Other Information
- The function creates copies rather than modifying nodes in-place, ensuring the original tree remains unchanged
- Includes stack depth checking to prevent overflow on deeply nested expressions
- Handles special cases for frequently used nodes (Var, Const) with optimized copying
- For SubLink nodes, it recurses into testexpr but preserves the link to sub-Query nodes unless the mutator handles Query nodes specially
- [SubPlan](../S/SubPlan.md) nodes have their testexpr and args processed, but the inner plan link is simply copied
- The function covers all node types typically found in target lists and qualifier clauses during query planning
- Uses efficient memory allocation with palloc and memcpy for node copying
- Returns NULL for NULL input nodes
- Terminates with error for unrecognized node types to ensure comprehensive coverage

## Simplified Source

```c
Node *expression_tree_mutator_impl(Node *node, tree_mutator_callback mutator, void *context)
{
    // Define mutator macros for cleaner code
    #define FLATCOPY(newnode, node, nodetype) \
        ((newnode) = (nodetype *) palloc(sizeof(nodetype)), \
         memcpy((newnode), (node), sizeof(nodetype)))

    #define MUTATE(newfield, oldfield, fieldtype) \
        ((newfield) = (fieldtype) mutator((Node *) (oldfield), context))

    if (node == NULL)
        return NULL;

    // Protect against stack overflow
    check_stack_depth();

    switch (nodeTag(node)) {
        // Primitive nodes - special handling for frequent types
        case T_Var:
            {
                Var *var = (Var *) node;
                Var *newnode;
                FLATCOPY(newnode, var, Var);
                return (Node *) newnode;
            }
        case T_Const:
            {
                Const *oldnode = (Const *) node;
                Const *newnode;
                FLATCOPY(newnode, oldnode, Const);
                return (Node *) newnode;
            }

        // Simple primitive nodes - use copyObject
        case T_Param:
        case T_CaseTestExpr:
        case T_SQLValueFunction:
        case T_CoerceToDomainValue:
        case T_SetToDefault:
        case T_CurrentOfExpr:
        case T_NextValueExpr:
        case T_RangeTblRef:
        case T_SortGroupClause:
            return (Node *) copyObject(node);

        // Simple nodes with single expression field
        case T_WithCheckOption:
            {
                WithCheckOption *wco = (WithCheckOption *) node;
                WithCheckOption *newnode;
                FLATCOPY(newnode, wco, WithCheckOption);
                MUTATE(newnode->qual, wco->qual, Node *);
                return (Node *) newnode;
            }
        case T_NamedArgExpr:
            {
                NamedArgExpr *nexpr = (NamedArgExpr *) node;
                NamedArgExpr *newnode;
                FLATCOPY(newnode, nexpr, NamedArgExpr);
                MUTATE(newnode->arg, nexpr->arg, Expr *);
                return (Node *) newnode;
            }
        case T_FieldSelect:
        case T_RelabelType:
        case T_CoerceViaIO:
        case T_ConvertRowtypeExpr:
        case T_CollateExpr:
        case T_NullTest:
        case T_BooleanTest:
        case T_CoerceToDomain:
            {
                // Generic single-argument node handling
                GenericExprNode *expr = (GenericExprNode *) node;
                GenericExprNode *newnode;
                FLATCOPY(newnode, expr, GenericExprNode);
                MUTATE(newnode->arg, expr->arg, Expr *);
                return (Node *) newnode;
            }

        // Function/operator expressions with argument lists
        case T_FuncExpr:
            {
                FuncExpr *expr = (FuncExpr *) node;
                FuncExpr *newnode;
                FLATCOPY(newnode, expr, FuncExpr);
                MUTATE(newnode->args, expr->args, List *);
                return (Node *) newnode;
            }
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
        case T_ScalarArrayOpExpr:
        case T_BoolExpr:
            {
                // Generic argument list handling
                OpExpr *expr = (OpExpr *) node;
                OpExpr *newnode;
                FLATCOPY(newnode, expr, OpExpr);
                MUTATE(newnode->args, expr->args, List *);
                return (Node *) newnode;
            }

        // Aggregate functions
        case T_Aggref:
            {
                Aggref *aggref = (Aggref *) node;
                Aggref *newnode;
                FLATCOPY(newnode, aggref, Aggref);
                newnode->aggargtypes = list_copy(aggref->aggargtypes);
                MUTATE(newnode->aggdirectargs, aggref->aggdirectargs, List *);
                MUTATE(newnode->args, aggref->args, List *);
                MUTATE(newnode->aggorder, aggref->aggorder, List *);
                MUTATE(newnode->aggdistinct, aggref->aggdistinct, List *);
                MUTATE(newnode->aggfilter, aggref->aggfilter, Expr *);
                return (Node *) newnode;
            }

        // Window functions
        case T_WindowFunc:
            {
                WindowFunc *wfunc = (WindowFunc *) node;
                WindowFunc *newnode;
                FLATCOPY(newnode, wfunc, WindowFunc);
                MUTATE(newnode->args, wfunc->args, List *);
                MUTATE(newnode->aggfilter, wfunc->aggfilter, Expr *);
                return (Node *) newnode;
            }

        // Subqueries and subplans
        case T_SubLink:
            {
                SubLink *sublink = (SubLink *) node;
                SubLink *newnode;
                FLATCOPY(newnode, sublink, SubLink);
                MUTATE(newnode->testexpr, sublink->testexpr, Node *);
                MUTATE(newnode->subselect, sublink->subselect, Node *);
                return (Node *) newnode;
            }
        case T_SubPlan:
            {
                SubPlan *subplan = (SubPlan *) node;
                SubPlan *newnode;
                FLATCOPY(newnode, subplan, SubPlan);
                MUTATE(newnode->testexpr, subplan->testexpr, Node *);
                MUTATE(newnode->args, subplan->args, List *);
                return (Node *) newnode;
            }

        // Case expressions
        case T_CaseExpr:
            {
                CaseExpr *caseexpr = (CaseExpr *) node;
                CaseExpr *newnode;
                FLATCOPY(newnode, caseexpr, CaseExpr);
                MUTATE(newnode->arg, caseexpr->arg, Expr *);
                MUTATE(newnode->args, caseexpr->args, List *);
                MUTATE(newnode->defresult, caseexpr->defresult, Expr *);
                return (Node *) newnode;
            }

        // Array and row expressions
        case T_ArrayExpr:
            return mutator((Node *) copyObject(node), context);
        case T_RowExpr:
        case T_CoalesceExpr:
        case T_MinMaxExpr:
            {
                // Generic list-based expressions
                ListExpr *expr = (ListExpr *) node;
                ListExpr *newnode;
                FLATCOPY(newnode, expr, ListExpr);
                MUTATE(newnode->args, expr->args, List *);
                return (Node *) newnode;
            }

        // Target entries
        case T_TargetEntry:
            {
                TargetEntry *tle = (TargetEntry *) node;
                TargetEntry *newnode;
                FLATCOPY(newnode, tle, TargetEntry);
                MUTATE(newnode->expr, tle->expr, Expr *);
                return (Node *) newnode;
            }

        // Query nodes - return unmodified per design
        case T_Query:
            return (Node *) copyObject(node);

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            break;
    }

    return NULL;
}
```

This simplified version reduces the original ~800 lines to ~150 lines (~19% of original size) while preserving the essential mutation algorithm. Key simplifications:

- Grouped similar node types using generic handling patterns
- Removed many specialized JSON, XML, and complex expression cases
- Kept the core FLATCOPY + MUTATE pattern for each node category
- Preserved critical cases like Var, Const, Aggref, SubLink that demonstrate different patterns
- Maintained the fundamental copy-and-mutate semantics
- Kept stack depth protection and error handling for unknown types
- Preserved the macro definitions that are essential to the pattern