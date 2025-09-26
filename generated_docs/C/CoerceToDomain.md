# CoerceToDomain

## Location
[src/include/nodes/primnodes.h:2025-2037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2025-L2037)

## Overview
CoerceToDomain represents the operation of coercing a value to a domain type in PostgreSQL, performing runtime constraint validation and returning the coerced result or raising an error if constraints are violated.

## Definition

```c
typedef struct CoerceToDomain
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* domain type ID (result type) */
	/* output typmod (currently always -1) */
	int32		resulttypmod pg_node_attr(query_jumble_ignore);
	/* OID of collation, or InvalidOid if none */
	Oid			resultcollid pg_node_attr(query_jumble_ignore);
	/* how to display this node */
	CoercionForm coercionformat pg_node_attr(query_jumble_ignore);
	ParseLoc	location;		/* token location, or -1 if unknown */
} CoerceToDomain;
```
## Detailed Description
CoerceToDomain is a specialized expression node that handles coercion of values to PostgreSQL domain types. Unlike simple type coercion, domain coercion involves runtime validation of domain constraints (such as CHECK constraints, NOT NULL constraints, etc.). The operation occurs at runtime rather than compile time because the precise set of constraints to be checked is determined dynamically.

If the input value satisfies all domain constraints, it is returned as the result with the domain type. If any constraint is violated, an error is raised. In scenarios where no constraints need to be applied, this operation is functionally equivalent to RelabelType.

## Parameters / Member Variables
- : Base Expr node structure
- : Input expression to be coerced to the domain type
- : OID of the target domain type (result type)
- : Output type modifier (currently always -1 for domains)
- : OID of the result collation, or InvalidOid if no collation applies
- : How to display this coercion (COERCE_EXPLICIT_CALL, COERCE_EXPLICIT_CAST, COERCE_IMPLICIT_CAST, COERCE_SQL_SYNTAX)
- : Token location in source query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - CoercionForm (enum for display formatting)
  - ParseLoc (for location tracking)
  - Expr (base expression structure)
  - Oid (object identifier type)
  
- Called from (representative examples):
  - ExecInitCoerceToDomain (executor initialization for domain coercion)
  - coerce_to_domain (parser function for creating domain coercions)
  - transformInsertRow (handling INSERT statement type coercion)
  - process_matched_tle (rewrite system for target list entries)
  - get_rule_expr (rule decompilation)

## Notes and Other Information
- Essential for implementing PostgreSQL's domain type system with constraint enforcement
- The pg_node_attr(query_jumble_ignore) annotations on resulttypmod, resultcollid, and coercionformat indicate these fields should be ignored during query fingerprinting
- Runtime constraint checking makes this more expensive than simple RelabelType operations
- Used extensively in the parser for type coercion, in the rewrite system for view updates, and in the executor for runtime type checking
- Critical for maintaining data integrity when working with user-defined domain types
- The coercionformat field is semantically ignored by equal() comparisons, allowing the planner to treat different display formats as equivalent