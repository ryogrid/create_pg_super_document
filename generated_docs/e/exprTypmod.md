# exprTypmod

## Location
[src/backend/nodes/nodeFuncs.c:298-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L298-L551)

## Overview
Returns the type-specific modifier (typmod) of an expression's result type, if it can be determined, otherwise returns -1.

## Definition

```c
structorExpr:
			return ((const JsonConstructorExpr *) expr)->returning->typmod;
```
## Detailed Description
The  function extracts the type modifier information from PostgreSQL expression nodes. Type modifiers provide additional constraints on data types, such as precision and scale for numeric types, length for character types, or other type-specific parameters.

Unlike  which can always determine a type,  often returns -1 when the type modifier cannot be determined or is not meaningful for the expression type. The function handles various expression types:

- For basic expressions (Var, Const, Param), it returns the stored typmod directly
- For function expressions, it attempts to detect length-coercion functions using 
- For complex expressions like CASE, COALESCE, ARRAY, and MIN/MAX, it checks if all alternatives have the same typmod and returns it, otherwise returns -1
- For subqueries, it recursively determines the typmod of the first target column
- For type coercion expressions, it returns the result typmod

The function implements a conservative approach: when there's any ambiguity about the typmod, it returns -1 rather than making assumptions.

## Parameters / Member Variables
- : A const pointer to the Node representing the expression whose type modifier should be determined. If NULL, returns -1.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine expression node type)
  - [exprIsLengthCoercion](exprIsLengthCoercion.md) (to detect length-coercion functions)
  - [exprTypmod](exprTypmod.md) (recursive calls for nested expressions)
  - [exprType](exprType.md) (for type checking in complex expressions)
  - linitial, linitial_node (for accessing list elements)
  - lfirst, lfirst_node (for list iteration)
  - for_each_from (for iterating from specific list positions)

- Called from (representative examples):
  - Type coercion and checking functions (coerce_type_typmod, select_common_typmod)
  - [Query](../Q/Query.md) planning and optimization (set_rel_width, get_expr_width)
  - Tuple descriptor construction (ConstructTupleDescriptor, ExecTypeFromTLInternal)
  - Parser functions for expressions and target lists

## Notes and Other Information
- Returns -1 for NULL input expressions or when typmod cannot be determined
- For length-coercion functions, uses  to extract the coerced typmod
- [Complex](../C/Complex.md) expressions (CASE, COALESCE, ARRAY, MIN/MAX) require all alternatives to agree on typmod
- For subqueries, array vs. non-array distinction doesn't affect typmod handling
- Type modifiers are crucial for proper type coercion and storage decisions
- The function is widely used throughout PostgreSQL's type system, particularly in query planning and execution
- Located in src/backend/nodes/nodeFuncs.c:298-551