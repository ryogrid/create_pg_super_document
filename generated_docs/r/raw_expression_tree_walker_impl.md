# raw_expression_tree_walker_impl

## Location
[src/backend/nodes/nodeFuncs.c:3964-4675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L3964-L4675)

## Overview
A comprehensive tree walker function that traverses raw parse trees (pre-analysis) for DML statements, handling all node types found in raw grammar output.

## Definition

```c
structorExpr:
			{
				JsonConstructorExpr *ctor = (JsonConstructorExpr *) node;

				if (WALK(ctor->args))
					return true;
				if (WALK(ctor->func))
					return true;
				if (WALK(ctor->coercion))
					return true;
				if (WALK(ctor->returning))
					return true;
			}
			break;
```
## Detailed Description
The  function provides tree traversal capabilities for raw parse trees, which are the direct output of the PostgreSQL grammar parser before semantic analysis. Unlike the regular , this function operates on unprocessed syntax trees and includes handling for all node types that can appear in raw DML statements (SELECT/INSERT/UPDATE/DELETE/MERGE).

The function implements a comprehensive switch statement covering over 60 different node types, from primitive literals and expressions to complex statement structures. It recursively walks through sub-nodes using the  macro, respecting the structure of each node type. The function includes extensive support for JSON operations, table functions, CTEs, and various SQL constructs.

This walker is particularly important during CTE analysis and other early-stage query processing where the system needs to examine raw parse tree structures before they undergo semantic transformation.

## Parameters
- `node`: The root node of the raw parse tree to traverse
- `walker`: Callback function that defines the walking behavior for each visited node
- `context`: Opaque context pointer passed through to the walker callback

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - nodeTag (node type identification macro)
  - WALK (recursive traversal macro)
  - elog (error logging for unrecognized nodes)
  - Various node type constants (T_JsonFormat, T_SelectStmt, etc.)
- Called from (representative examples):
  - raw_expression_tree_walker (wrapper function)
  - planstate_tree_walker (indirectly via wrapper)

## Notes and Other Information
- Returns boolean indicating whether the walk should terminate early (true) or continue (false)
- Unlike , this function has no special query boundary rules and descends into all potentially interesting nodes
- Covers extensive JSON functionality including JSON path expressions, JSON table functions, and JSON constructors
- Includes stack depth checking to prevent overflow on deeply nested expressions
- [Node](../N/Node.md) type coverage is specifically focused on DML statements as these are the primary use case for CTE analysis
- Primitive node types (literals, constants, parameters) are handled as leaf nodes with no further traversal
- Located in src/backend/nodes/nodeFuncs.c:3964-4675

## Simplified Source

```c
bool
raw_expression_tree_walker_impl(Node *node, tree_walker_callback walker, void *context)
{
    // Handle null input
    if (node == NULL)
        return false;

    // Prevent stack overflow on deeply nested expressions
    check_stack_depth();

    // Main dispatch based on node type
    switch (nodeTag(node))
    {
        // Primitive leaf nodes - no sub-nodes to traverse
        case T_Integer:
        case T_Float:
        case T_Boolean:
        case T_String:
        case T_ParamRef:
        case T_A_Const:
        case T_A_Star:
            break;

        // Basic expression types
        case T_SubLink:
            return WALK(((SubLink *) node)->testexpr) ||
                   WALK(((SubLink *) node)->subselect);

        case T_CaseExpr:
            return handle_case_expression_walking(node);

        case T_BoolExpr:
            return WALK(((BoolExpr *) node)->args);

        // DML Statements
        case T_SelectStmt:
            return walk_select_statement(node);

        case T_InsertStmt:
            return walk_insert_statement(node);

        case T_UpdateStmt:
            return walk_update_statement(node);

        case T_DeleteStmt:
            return walk_delete_statement(node);

        // JSON operations (extensive support)
        case T_JsonFuncExpr:
        case T_JsonConstructorExpr:
        case T_JsonTable:
            return walk_json_operations(node);

        // Function calls and operators
        case T_FuncCall:
            return walk_function_call(node);

        case T_A_Expr:
            return WALK(((A_Expr *) node)->lexpr) ||
                   WALK(((A_Expr *) node)->rexpr);

        // Complex structural nodes
        case T_List:
            return walk_list_elements(node);

        case T_JoinExpr:
            return walk_join_expression(node);

        // Type and casting operations
        case T_TypeCast:
            return WALK(((TypeCast *) node)->arg) ||
                   WALK(((TypeCast *) node)->typeName);

        // Range and table references
        case T_RangeVar:
        case T_RangeSubselect:
        case T_RangeFunction:
            return walk_range_expressions(node);

        // Common Table Expressions
        case T_WithClause:
            return WALK(((WithClause *) node)->ctes);

        case T_CommonTableExpr:
            return WALK(((CommonTableExpr *) node)->ctequery);

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            break;
    }

    return false;
}
```