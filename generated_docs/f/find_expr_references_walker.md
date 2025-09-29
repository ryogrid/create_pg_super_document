# find_expr_references_walker

## Location
[src/backend/catalog/dependency.c:1698-2320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L1698-L2320)

## Overview
Recursively traverses an expression tree to identify and collect all database object references, serving as the core dependency discovery engine for PostgreSQL's dependency tracking system.

## Definition

```c
structure */
	}
	else if (IsA(node, CTECycleClause))
	{
		CTECycleClause *cc = (CTECycleClause *) node;
```
## Detailed Description
This function is the heart of PostgreSQL's expression dependency analysis system. It implements a comprehensive tree walker that recursively examines every node in an expression tree to identify references to database objects such as tables, columns, operators, functions, types, collations, and more.

The function handles dozens of different node types, each with specific logic for extracting object references. It employs intelligent dependency tracking to avoid redundant dependencies - for instance, it doesn't create type dependencies when there's already an indirect dependency through an operator or function.

Key responsibilities include:
- Variable (Var) resolution through range tables 
- Constant value analysis for reg* type literals
- Function and operator reference extraction
- Type and collation dependency tracking
- Subquery and CTE processing
- Complex expression type handling (FieldSelect, ArrayCoerceExpr, etc.)
- Special handling for INSERT/UPDATE target columns

The walker uses the standard PostgreSQL expression_tree_walker framework and maintains context about the current range table stack for proper variable resolution across nested queries.

## Parameters / Member Variables
- : Current node in the expression tree being examined
- : Context structure containing collected object addresses and range table stack

## Dependencies
- Functions called/Symbols referenced:
  - [add_object_address](../a/add_object_address.md) (primary dependency recording function)
  - expression_tree_walker (recursive tree traversal framework)
  - query_tree_walker (for Query node traversal)
  - [process_function_rte_ref](../p/process_function_rte_ref.md) (for function RTE column analysis)
  - [getBaseType](../g/getBaseType.md), get_typ_typrelid (type analysis utilities)
  - SearchSysCacheExists1 (catalog existence checks)
  - Various list manipulation functions (list_nth, lcons, etc.)
- Called from (representative examples):
  - [recordDependencyOnExpr](../r/recordDependencyOnExpr.md) (main entry point)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md) (single-relation variant)
  - Self-recursion for complex nested structures

## Notes and Other Information
- Handles over 25 different expression node types with specialized logic for each
- Implements optimized dependency tracking to avoid redundant type/collation dependencies
- Uses range table context stack to properly resolve variables across query nesting levels
- Special handling for reg* literal types that reference specific database objects
- Automatically processes subqueries and CTEs recursively
- Critical for maintaining referential integrity in PostgreSQL's catalog system
- The function deliberately avoids creating dependencies for whole-row references, leaving that responsibility to the range table level
- Contains extensive error handling for malformed expressions and invalid references

## Simplified Source

```c
static bool find_expr_references_walker(Node *node, find_expr_references_context *context) {
    if (node == NULL)
        return false;

    // Handle different expression node types
    if (IsA(node, Var)) {
        // Variable reference: add column dependency
        Var *var = (Var *) node;
        RangeTblEntry *rte = get_rte_from_context(var, context);

        if (var->varattno != InvalidAttrNumber && rte->rtekind == RTE_RELATION) {
            add_object_address(RelationRelationId, rte->relid, var->varattno, context->addrs);
        }
        return false;
    }

    if (IsA(node, Const)) {
        // Constant: add type dependency and handle reg* literals
        Const *con = (Const *) node;
        add_object_address(TypeRelationId, con->consttype, 0, context->addrs);

        // Handle reg* types that reference specific objects
        if (!con->constisnull) {
            handle_regtype_constant(con, context);
        }
        return false;
    }

    if (IsA(node, FuncExpr)) {
        // Function call: add function dependency
        FuncExpr *funcexpr = (FuncExpr *) node;
        add_object_address(ProcedureRelationId, funcexpr->funcid, 0, context->addrs);
        // Fall through to examine arguments
    }

    if (IsA(node, OpExpr)) {
        // Operator: add operator dependency
        OpExpr *opexpr = (OpExpr *) node;
        add_object_address(OperatorRelationId, opexpr->opno, 0, context->addrs);
        // Fall through to examine arguments
    }

    if (IsA(node, Aggref)) {
        // Aggregate function: add aggregate dependency
        Aggref *aggref = (Aggref *) node;
        add_object_address(ProcedureRelationId, aggref->aggfnoid, 0, context->addrs);
        // Fall through to examine arguments
    }

    if (IsA(node, FieldSelect)) {
        // Field selection: add column or type dependency
        FieldSelect *fselect = (FieldSelect *) node;
        Oid reltype = get_typ_typrelid(getBaseType(exprType((Node *) fselect->arg)));

        if (OidIsValid(reltype)) {
            add_object_address(RelationRelationId, reltype, fselect->fieldnum, context->addrs);
        } else {
            add_object_address(TypeRelationId, fselect->resulttype, 0, context->addrs);
        }
    }

    if (IsA(node, Query)) {
        // Subquery: process range table and recurse
        Query *query = (Query *) node;

        // Add dependencies for relations in range table
        foreach_rte_in_query(query, context);

        // Add target column dependencies for INSERT/UPDATE
        if (query->commandType == CMD_INSERT || query->commandType == CMD_UPDATE) {
            add_target_column_dependencies(query, context);
        }

        // Recurse into subquery structure
        context->rtables = lcons(query->rtable, context->rtables);
        bool result = query_tree_walker(query, find_expr_references_walker, context,
                                       QTW_IGNORE_JOINALIASES | QTW_EXAMINE_SORTGROUP);
        context->rtables = list_delete_first(context->rtables);
        return result;
    }

    // Handle type coercion expressions
    if (IsA(node, RelabelType) || IsA(node, CoerceViaIO) || IsA(node, ArrayCoerceExpr)) {
        add_type_dependency_from_coercion(node, context);
        // Fall through for some types
    }

    // For all other node types, continue tree walking
    return expression_tree_walker(node, find_expr_references_walker, context);
}
```