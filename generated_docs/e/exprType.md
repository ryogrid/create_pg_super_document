# exprType

## Location
[src/backend/nodes/nodeFuncs.c:42-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L42-L297)

## Overview
Returns the Oid of the type of the given expression's result, handling all PostgreSQL expression node types.

## Definition

```c
structorExpr:
			type = ((const JsonConstructorExpr *) expr)->returning->typid;
```
## Detailed Description
The  function is a central utility in PostgreSQL's expression handling system that determines the data type (as an Oid) of any expression node. It performs a comprehensive switch statement over all possible expression node types, extracting the appropriate type information from each node's type-specific fields.

The function handles a wide variety of expression types including:
- Basic expression types (Var, Const, Param)
- Function and operator expressions (FuncExpr, OpExpr, Aggref)
- Subqueries and subplans (SubLink, SubPlan)
- Type coercion expressions (RelabelType, CoerceViaIO, ArrayCoerceExpr)
- Control flow expressions (CaseExpr, CoalesceExpr)
- JSON and XML expressions
- Array and row expressions
- Boolean test expressions

For complex expressions like subqueries, the function recursively determines types and handles special cases like array sublinks by promoting the element type to an array type.

## Parameters / Member Variables
- : A const pointer to the Node representing the expression whose type should be determined. If NULL, returns InvalidOid.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine expression node type)
  - linitial_node (for accessing first target entry in subqueries)
  - [get_promoted_array_type](../g/get_promoted_array_type.md) (for array sublink type promotion)
  - [format_type_be](../f/format_type_be.md) (for error reporting)
  - [exprType](exprType.md) (recursive calls for nested expressions)
  
- Called from (representative examples):
  - Expression planning and optimization functions
  - Type checking and coercion functions
  - [Query](../Q/Query.md) transformation utilities

## Notes and Other Information
- Returns InvalidOid for NULL input expressions
- Throws an ERROR for unrecognized node types
- For ARRAY_SUBLINK expressions, promotes the element type to the corresponding array type
- MULTIEXPR_SUBLINK always returns RECORDOID
- Most boolean operations and tests return BOOLOID
- The function is essential for PostgreSQL's type system and is used extensively throughout query processing
- Located in src/backend/nodes/nodeFuncs.c:42-297

## Simplified Source

```c
Oid
exprType(const Node *expr)
{
    if (!expr)
        return InvalidOid;

    // Main dispatch switch on node type
    switch (nodeTag(expr))
    {
        // Basic expression types - extract type directly
        case T_Var:
            return ((const Var *) expr)->vartype;
        case T_Const:
            return ((const Const *) expr)->consttype;
        case T_Param:
            return ((const Param *) expr)->paramtype;

        // Function and operation types
        case T_FuncExpr:
            return ((const FuncExpr *) expr)->funcresulttype;
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
            return ((const OpExpr *) expr)->opresulttype;
        case T_Aggref:
            return ((const Aggref *) expr)->aggtype;

        // Boolean result types
        case T_BoolExpr:
        case T_ScalarArrayOpExpr:
        case T_RowCompareExpr:
        case T_NullTest:
        case T_BooleanTest:
            return BOOLOID;

        // Subqueries - handle different sublink types
        case T_SubLink:
        case T_SubPlan:
            return handle_subquery_type(expr);

        // Type coercion expressions
        case T_RelabelType:
        case T_CoerceViaIO:
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_CoerceToDomain:
            return get_coercion_result_type(expr);

        // Recursive cases - delegate to nested expressions
        case T_NamedArgExpr:
        case T_CollateExpr:
        case T_InferenceElem:
        case T_PlaceHolderVar:
            return exprType(get_nested_expr(expr));

        // Complex expression types with specific handling
        case T_CaseExpr:
            return ((const CaseExpr *) expr)->casetype;
        case T_ArrayExpr:
            return ((const ArrayExpr *) expr)->array_typeid;
        case T_RowExpr:
            return ((const RowExpr *) expr)->row_typeid;

        // JSON/XML expressions
        case T_JsonExpr:
        case T_JsonConstructorExpr:
        case T_XmlExpr:
            return handle_json_xml_type(expr);

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
            return InvalidOid;
    }
}
```