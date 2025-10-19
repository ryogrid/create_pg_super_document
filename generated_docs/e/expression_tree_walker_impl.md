# expression_tree_walker_impl

## Location
[src/backend/nodes/nodeFuncs.c:2083-2096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L2083-L2096)

## Overview
The core implementation function that provides generic tree-walking logic for traversing PostgreSQL expression trees in a read-only fashion.

## Definition

```c
bool
expression_tree_walker_impl(Node *node,
							tree_walker_callback walker,
							void *context)
```
## Detailed Description
This function is the heart of PostgreSQL's expression tree traversal infrastructure. It implements a comprehensive switch statement that handles dozens of different node types, recursively calling the provided walker function on all expression subnodes. The function eliminates the need for duplicate tree-walking code across different routines by providing a centralized, well-tested implementation.

The function uses two key macros:
- : Calls the walker function on a single node
- : Recursively processes list nodes by calling expression_tree_walker_impl

Key behaviors:
- Returns false to continue traversal, true to abort and propagate upward
- Guards against stack overflow with check_stack_depth()
- Handles primitive nodes (Var, Const, etc.) with no recursion
- Processes complex nodes by walking their expression subnodes
- For SubLink nodes, walks testexpr and calls walker on the sub-Query
- For List nodes, recurses directly without calling the walker

The function handles all major expression node types including operators, functions, aggregates, window functions, subqueries, joins, and many specialized constructs.

## Parameters / Member Variables
- `*node`: The current node being processed in the expression tree
- `walker`: Callback function that processes nodes and returns bool for continuation control
- `*context`: Arbitrary context data passed through to walker calls
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - nodeTag
  - elog
  - Various macros: WALK, LIST_WALK, IsA
- Called from (representative examples):
  - expression_tree_walker (macro wrapper)
  - planstate_tree_walker
  - LIST_WALK (recursive calls)

## Notes and Other Information
- This is the implementation function; most code uses the expression_tree_walker macro wrapper
- Supports read-only traversal and in-place node modification but not node replacement
- Comprehensive coverage of PostgreSQL expression node types
- Essential infrastructure used throughout the query planner and optimizer
- Companion to expression_tree_mutator for tree modification scenarios
- The walker callback controls traversal: false continues, true aborts
- Includes stack depth protection against deeply nested expressions

## Simplified Source

```c
bool expression_tree_walker_impl(Node *node, tree_walker_callback walker, void *context)
{
    // Define walker macros for cleaner code
    #define WALK(n) walker((Node *) (n), context)
    #define LIST_WALK(l) expression_tree_walker_impl((Node *) (l), walker, context)

    if (node == NULL)
        return false;

    // Protect against stack overflow
    check_stack_depth();

    switch (nodeTag(node))
    {
        // Primitive nodes - no recursion needed
        case T_Var:
        case T_Const:
        case T_Param:
        case T_CaseTestExpr:
        case T_SQLValueFunction:
        case T_CurrentOfExpr:
        case T_NextValueExpr:
        case T_RangeTblRef:
            break;

        // Simple nodes with single expression
        case T_WithCheckOption:
            return WALK(((WithCheckOption *) node)->qual);
        case T_NamedArgExpr:
            return WALK(((NamedArgExpr *) node)->arg);
        case T_FieldSelect:
            return WALK(((FieldSelect *) node)->arg);
        case T_RelabelType:
            return WALK(((RelabelType *) node)->arg);
        case T_CoerceViaIO:
            return WALK(((CoerceViaIO *) node)->arg);
        case T_ConvertRowtypeExpr:
            return WALK(((ConvertRowtypeExpr *) node)->arg);
        case T_CollateExpr:
            return WALK(((CollateExpr *) node)->arg);
        case T_NullTest:
            return WALK(((NullTest *) node)->arg);
        case T_BooleanTest:
            return WALK(((BooleanTest *) node)->arg);
        case T_CoerceToDomain:
            return WALK(((CoerceToDomain *) node)->arg);
        case T_TargetEntry:
            return WALK(((TargetEntry *) node)->expr);
        case T_PlaceHolderVar:
            return WALK(((PlaceHolderVar *) node)->phexpr);
        case T_InferenceElem:
            return WALK(((InferenceElem *) node)->expr);

        // Aggregate functions - walk all argument lists
        case T_Aggref:
            {
                Aggref *expr = (Aggref *) node;
                if (LIST_WALK(expr->aggdirectargs)) return true;
                if (LIST_WALK(expr->args)) return true;
                if (LIST_WALK(expr->aggorder)) return true;
                if (LIST_WALK(expr->aggdistinct)) return true;
                if (WALK(expr->aggfilter)) return true;
            }
            break;

        // Function expressions - walk argument lists
        case T_FuncExpr:
            return LIST_WALK(((FuncExpr *) node)->args);
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
            return LIST_WALK(((OpExpr *) node)->args);
        case T_ScalarArrayOpExpr:
            return LIST_WALK(((ScalarArrayOpExpr *) node)->args);
        case T_BoolExpr:
            return LIST_WALK(((BoolExpr *) node)->args);

        // Window functions
        case T_WindowFunc:
            {
                WindowFunc *expr = (WindowFunc *) node;
                if (LIST_WALK(expr->args)) return true;
                if (WALK(expr->aggfilter)) return true;
                if (WALK(expr->runCondition)) return true;
            }
            break;

        // Subqueries and subplans
        case T_SubLink:
            {
                SubLink *sublink = (SubLink *) node;
                if (WALK(sublink->testexpr)) return true;
                return WALK(sublink->subselect); // Walk the sub-query
            }
            break;
        case T_SubPlan:
            {
                SubPlan *subplan = (SubPlan *) node;
                if (WALK(subplan->testexpr)) return true;
                if (LIST_WALK(subplan->args)) return true;
            }
            break;

        // Case expressions - walk all branches
        case T_CaseExpr:
            {
                CaseExpr *caseexpr = (CaseExpr *) node;
                ListCell *temp;

                if (WALK(caseexpr->arg)) return true;
                foreach(temp, caseexpr->args)
                {
                    CaseWhen *when = lfirst_node(CaseWhen, temp);
                    if (WALK(when->expr)) return true;
                    if (WALK(when->result)) return true;
                }
                if (WALK(caseexpr->defresult)) return true;
            }
            break;

        // Array and row expressions
        case T_ArrayExpr:
            return WALK(((ArrayExpr *) node)->elements);
        case T_RowExpr:
            return WALK(((RowExpr *) node)->args);
        case T_CoalesceExpr:
            return WALK(((CoalesceExpr *) node)->args);
        case T_MinMaxExpr:
            return WALK(((MinMaxExpr *) node)->args);

        // Lists - recurse through all elements
        case T_List:
            {
                ListCell *temp;
                foreach(temp, (List *) node)
                {
                    if (WALK(lfirst(temp))) return true;
                }
            }
            break;

        // Join expressions
        case T_JoinExpr:
            {
                JoinExpr *join = (JoinExpr *) node;
                if (WALK(join->larg)) return true;
                if (WALK(join->rarg)) return true;
                if (WALK(join->quals)) return true;
            }
            break;

        // Query nodes - do nothing per design
        case T_Query:
            break;

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            break;
    }

    return false;

    #undef LIST_WALK
}
```

This simplified version reduces the original ~580 lines to ~140 lines (~24% of original size) while preserving the essential tree-walking algorithm. Key simplifications:

- Grouped similar node types together
- Removed many specialized JSON, XML, and partition-specific cases
- Kept the core pattern: primitive nodes (no recursion), simple nodes (single recursion), complex nodes (multiple recursions)
- Preserved the critical WALK/LIST_WALK macro pattern
- Maintained the essential control flow and error handling
- Kept stack depth protection and the fundamental switch-case structure