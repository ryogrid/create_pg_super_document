# SubscriptingRef

## Location
[src/include/nodes/primnodes.h:679-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L679-L704)

## Overview
SubscriptingRef describes a subscripting operation over a container (such as arrays), supporting both fetching and storing operations for single elements or slices of the container.

## Definition

```c
typedef struct SubscriptingRef
{
	Expr		xpr;
	/* type of the container proper */
	Oid			refcontainertype pg_node_attr(query_jumble_ignore);
	/* the container type's pg_type.typelem */
	Oid			refelemtype pg_node_attr(query_jumble_ignore);
	/* type of the SubscriptingRef's result */
	Oid			refrestype pg_node_attr(query_jumble_ignore);
	/* typmod of the result */
	int32		reftypmod pg_node_attr(query_jumble_ignore);
	/* collation of result, or InvalidOid if none */
	Oid			refcollid pg_node_attr(query_jumble_ignore);
	/* expressions that evaluate to upper container indexes */
	List	   *refupperindexpr;

	/*
	 * expressions that evaluate to lower container indexes, or NIL for single
	 * container element.
	 */
	List	   *reflowerindexpr;
	/* the expression that evaluates to a container value */
	Expr	   *refexpr;
	/* expression for the source value, or NULL if fetch */
	Expr	   *refassgnexpr;
} SubscriptingRef;
```
## Detailed Description
SubscriptingRef is a comprehensive expression node that handles all forms of subscripting operations on container types in PostgreSQL, primarily arrays but also other subscriptable types like JSONB. It supports four main operations: fetching single elements, fetching slices, storing single elements, and storing slices.

The structure can represent both simple subscripting (e.g., ) and slice operations (e.g., ). For slice operations, both lower and upper bounds are specified through separate expression lists. When  is NIL, the operation targets a single element; otherwise, it targets a slice.

The implementation allows for in-place modifications when dealing with read-write expanded containers, providing performance optimizations for large container operations.

## Parameters / Member Variables
- `xpr`: Base expression node structure (inherited from Expr)
- `pg_node_attr(query_jumble_ignore)`: OID of the actual container type that determines subscripting semantics
- `pg_node_attr(query_jumble_ignore)`: OID of the container's element type (saved for subscripting functions)
- `pg_node_attr(query_jumble_ignore)`: OID of the SubscriptingRef operation's result type
- `pg_node_attr(query_jumble_ignore)`: Type modifier of the result
- `pg_node_attr(query_jumble_ignore)`: Collation OID of the result, or InvalidOid if none
- `*refupperindexpr`: List of expressions evaluating to upper container indexes
- `*reflowerindexpr`: List of expressions evaluating to lower container indexes (NIL for single element operations)
- `*refexpr`: Expression that evaluates to the container value being subscripted
- `*refassgnexpr`: Expression for the source value in assignment operations (NULL for fetch operations)
## Dependencies
- Functions called/Symbols referenced:
  - [Expr](../E/Expr.md) (base expression structure and container expressions)
  - [List](../L/List.md) (for index expressions)
  - Oid (for type references)
  
- Called from (representative examples):
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md) (executor initialization for subscripting operations)
  - [transformContainerSubscripts](../t/transformContainerSubscripts.md) (parser transformation of subscript expressions)
  - [array_subscript_transform](../a/array_subscript_transform.md), jsonb_subscript_transform (type-specific subscripting transformations)
  - [processIndirection](../p/processIndirection.md) (rule output processing for subscript operations)
  - [transformAssignmentSubscripts](../t/transformAssignmentSubscripts.md) (parser handling of assignment to subscripted containers)

## Notes and Other Information
- Supports both fetch and store operations - the presence of  determines the operation type
- Individual expressions in slice subscript lists can be NULL, meaning "use the container's current bound"
- Type information fields are marked as query_jumble_ignore for consistent plan caching
- For slice operations,  and  must have the same length when both are present
- The result type can vary: element type for single element fetch, container type for slice operations or stores
- Extensible design allows different container types to implement custom subscripting semantics
- Supports PostgreSQL's advanced array operations including multi-dimensional arrays and complex slice operations
- Performance-optimized for expanded container objects, allowing in-place modifications when possible