# ConvertRowtypeExpr

## Location
[src/include/nodes/primnodes.h:1258-1267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1258-L1267)

## Overview
ConvertRowtypeExpr represents a type coercion from one composite type to another, where columns are matched by name rather than position, primarily used for inheritance relationships.

## Definition

```c
typedef struct ConvertRowtypeExpr
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* output type (always a composite type) */
	/* Like RowExpr, we deliberately omit a typmod and collation here */
	/* how to display this node */
	CoercionForm convertformat pg_node_attr(query_jumble_ignore);
	ParseLoc	location;		/* token location, or -1 if unknown */
} ConvertRowtypeExpr;
```
## Detailed Description
ConvertRowtypeExpr handles coercion between composite types where the source type is guaranteed to contain all columns needed for the destination type, plus possibly additional ones. Unlike positional matching, columns are matched by name, making this suitable for inheritance scenarios where child tables may have additional columns beyond their parent.

The primary use case is converting whole-row values from inheritance child tables into valid whole-row values of their parent table's rowtype. For example, when a query expects a parent table's rowtype but receives a child table's row that has additional columns.

Key characteristics:
- Both source and result types must be named composite types (not domains)
- Source type must contain all columns present in the target type
- Column matching is done by name, not position
- Additional columns in the source are ignored during conversion
- Type modifiers and collations are deliberately omitted (like RowExpr)

## Parameters / Member Variables
- `xpr`: Base expression node structure
- `*arg`: Input expression yielding a composite value to be converted
- `resulttype`: OID of the target composite type
- `pg_node_attr(query_jumble_ignore)`: Controls how this conversion is displayed (ignored for query jumbling)
- `location`: Parse location in the original query, or -1 if unknown
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating ConvertRowtypeExpr instances)
  - [getBaseType](../g/getBaseType.md) (to handle domains over composite types)
  - [ExprEvalRowtypeCache](../E/ExprEvalRowtypeCache.md) (for execution-time caching)
  - Composite type manipulation functions
- Called from (representative examples):
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md) (in rewriteManip.c for inheritance handling)
  - [coerce_record_to_complex](../c/coerce_record_to_complex.md) (in parse_coerce.c for record coercion)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (during execution plan initialization)

## Notes and Other Information
- Designed specifically for inheritance relationships where child types extend parent types
- The executor uses caching structures (ExprEvalRowtypeCache) for efficient repeated conversions
- When input is a domain over a composite type, a RelabelType node is inserted to convert to the base type first
- Does not handle type modifier or collation conversions (these are omitted by design)
- Conversion can fail at runtime if required columns are missing from the source type
- Used internally by PostgreSQL's inheritance mechanism and whole-row variable handling
- The convertformat field follows the same conventions as other coercion expression types