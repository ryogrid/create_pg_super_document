# RelabelType

## Location
[src/include/nodes/primnodes.h:1181-1193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1181-L1193)

## Overview
RelabelType represents a "dummy" type coercion between two binary-compatible datatypes, serving as a no-op at runtime that provides a place to store the correct result type during type resolution.

## Definition

```c
typedef struct RelabelType
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* output type of coercion expression */
	/* output typmod (usually -1) */
	int32		resulttypmod pg_node_attr(query_jumble_ignore);
	/* OID of collation, or InvalidOid if none */
	Oid			resultcollid pg_node_attr(query_jumble_ignore);
	/* how to display this node */
	CoercionForm relabelformat pg_node_attr(query_jumble_ignore);
	ParseLoc	location;		/* token location, or -1 if unknown */
} RelabelType;
```
## Detailed Description
RelabelType implements "dummy" type coercions between binary-compatible datatypes, such as reinterpreting an OID expression result as int4. This node performs no actual runtime operation but serves as a critical component in PostgreSQL's type system by providing a location to store the correct type attribution for expression results during type resolution.

The need for RelabelType arises because simply overwriting the type field of an input expression node is insufficient - a separate node is required to explicitly show the coercion's result type. This maintains type system integrity while allowing efficient reinterpretation of compatible types.

The node is commonly used in situations where the physical representation of data remains the same but the logical type interpretation changes, such as between related types like OID and int4, or when dealing with domain types and their base types.

## Parameters / Member Variables
- : Base Expr node structure
- : Input expression to be reinterpreted
- : OID of the output type after coercion
- : Type modifier for the result type (usually -1)
- : OID of collation for the result, or InvalidOid if none
- : CoercionForm indicating how this coercion should be displayed
- : Parse location of the coercion in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - CoercionForm
  - ParseLoc
- Called from (representative examples):
  - coerce_type
  - makeRelabelType
  - hide_coercion_node
  - strip_implicit_coercions
  - match_index_to_operand

## Notes and Other Information
- This is a runtime no-op - no actual data conversion occurs
- Essential for maintaining type system correctness with binary-compatible types
- The resulttypmod, resultcollid, and relabelformat fields have query_jumble_ignore attributes
- Commonly used for OID ↔ int4 conversions and domain type handling
- The relabelformat field controls how the coercion is displayed in query output
- Location information helps with error reporting and debugging
- Used extensively in the optimizer for type compatibility checking and plan generation
- Critical for supporting PostgreSQL's flexible type system while maintaining performance