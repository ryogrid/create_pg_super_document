# exprType

## Location
src/backend/nodes/nodeFuncs.c: 42 - 297

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
  - get_promoted_array_type (for array sublink type promotion)
  - format_type_be (for error reporting)
  - exprType (recursive calls for nested expressions)
  
- Called from (representative examples):
  - Expression planning and optimization functions
  - Type checking and coercion functions
  - Query transformation utilities

## Notes and Other Information
- Returns InvalidOid for NULL input expressions
- Throws an ERROR for unrecognized node types
- For ARRAY_SUBLINK expressions, promotes the element type to the corresponding array type
- MULTIEXPR_SUBLINK always returns RECORDOID
- Most boolean operations and tests return BOOLOID
- The function is essential for PostgreSQL's type system and is used extensively throughout query processing
- Located in src/backend/nodes/nodeFuncs.c:42-297