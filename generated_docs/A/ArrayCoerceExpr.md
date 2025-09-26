# ArrayCoerceExpr

## Location
[src/include/nodes/primnodes.h:1230-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1230-L1243)

## Overview
ArrayCoerceExpr represents a type coercion from one array type to another, implemented by applying per-element coercion to each array element using a specified element expression.

## Definition

```c
typedef struct ArrayCoerceExpr
{
	Expr		xpr;
	Expr	   *arg;			/* input expression (yields an array) */
	Expr	   *elemexpr;		/* expression representing per-element work */
	Oid			resulttype;		/* output type of coercion (an array type) */
	/* output typmod (also element typmod) */
	int32		resulttypmod pg_node_attr(query_jumble_ignore);
	/* OID of collation, or InvalidOid if none */
	Oid			resultcollid pg_node_attr(query_jumble_ignore);
	/* how to display this node */
	CoercionForm coerceformat pg_node_attr(query_jumble_ignore);
	ParseLoc	location;		/* token location, or -1 if unknown */
} ArrayCoerceExpr;
```
## Detailed Description
ArrayCoerceExpr handles type coercion between different array types by applying element-level transformations. The coercion works by iterating through each element of the source array and applying the  transformation to convert each element from the source element type to the target element type.

Within the , source elements are represented by CaseTestExpr nodes, which serve as placeholders for the actual array element values during execution. Even when the element coercion is minimal (like a simple RelabelType), the coercion process still requires work to update the element type OID stored in the array header.

The execution process involves:
1. Evaluating the source array expression
2. Extracting each element from the source array
3. Applying the element expression to transform each element
4. Constructing a new array with the transformed elements and updated type information

## Parameters / Member Variables
- : Base expression node structure
- : Input expression that yields an array to be coerced
- : Expression defining how to transform each individual array element
- : OID of the target array type
- : Type modifier for the result (also applies to elements, ignored for query jumbling)
- : OID of the result collation, or InvalidOid if none (ignored for query jumbling)
- : Controls how this coercion is displayed in query output
- : Parse location in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating ArrayCoerceExpr instances)
  - CaseTestExpr (used within elemexpr to represent source elements)
  - [get_element_type](../g/get_element_type.md) (to determine source element type)
  - [ExecEvalArrayCoerce](../E/ExecEvalArrayCoerce.md) (executor function)
  - Array manipulation functions
- Called from (representative examples):
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (when COERCION_PATH_ARRAYCOERCE is needed)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (during execution plan initialization)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md) (during constant folding)

## Notes and Other Information
- [ArrayCoerceExpr](ArrayCoerceExpr.md) is used when PostgreSQL determines COERCION_PATH_ARRAYCOERCE is the appropriate conversion method
- The elemexpr cannot contain nested CaseExpr or ArrayCoerceExpr nodes due to CaseTestExpr usage constraints
- Even for "no-op" coercions, the array header's element type OID must be updated
- The coercion preserves array dimensions and element ordering
- Performance considerations: operates on each array element individually, so large arrays may have significant overhead
- Commonly used when converting between compatible array types (e.g., text[] to varchar[])