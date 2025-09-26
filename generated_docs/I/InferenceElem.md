# InferenceElem

## Location
[src/include/nodes/primnodes.h:2123-2129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2123-L2129)

## Overview
A node representing an element of a unique index inference specification, used in INSERT ... ON CONFLICT statements to identify which unique index should be considered for conflict detection.

## Definition

```c
typedef struct InferenceElem
{
	Expr		xpr;
	Node	   *expr;			/* expression to infer from, or NULL */
	Oid			infercollid;	/* OID of collation, or InvalidOid */
	Oid			inferopclass;	/* OID of att opclass, or InvalidOid */
} InferenceElem;
```
## Detailed Description
InferenceElem represents individual elements within a unique index inference specification, primarily used in PostgreSQL's INSERT ... ON CONFLICT functionality. When PostgreSQL needs to determine which unique index to use for conflict detection, it analyzes InferenceElem nodes that describe the expressions, collations, and operator classes that should match a particular unique index.

The structure is similar to IndexElem used in utility commands, but InferenceElem is specifically designed for runtime index inference rather than index definition. During query planning, PostgreSQL examines these elements to find a unique index whose definition matches the specified inference elements, enabling proper conflict detection and handling.

## Parameters / Member Variables
- `xpr`: Base expression node structure (inherited from Expr)
- `*expr`: Expression to infer from (e.g., a column reference or expression), or NULL for simple column references
- `infercollid`: OID of the collation to match, or InvalidOid if no specific collation is required
- `inferopclass`: OID of the operator class to match, or InvalidOid if no specific operator class is required
## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [infer_arbiter_indexes](../i/infer_arbiter_indexes.md) (src/backend/optimizer/util/plancat.c:751)
  - [resolve_unique_index_expr](../r/resolve_unique_index_expr.md) (src/backend/parser/parse_clause.c:3210)
  - [infer_collation_opclass_match](../i/infer_collation_opclass_match.md) (src/backend/optimizer/util/plancat.c:978)
  - [exprType](../e/exprType.md)/exprCollation/exprLocation (src/backend/nodes/nodeFuncs.c)

## Notes and Other Information
- Used exclusively in INSERT ... ON CONFLICT statements for unique index inference
- The inference process matches these elements against existing unique indexes to find the appropriate arbiter index
- Both collation and operator class matching are optional - InvalidOid values indicate no specific requirement
- The expr field can contain complex expressions, not just simple column references
- Part of PostgreSQL's UPSERT functionality introduced in version 9.5
- Enables automatic selection of the correct unique constraint for conflict resolution
- The inference algorithm considers expression equivalence, collation compatibility, and operator class matching
- Critical for ensuring that ON CONFLICT clauses work correctly with the intended unique constraints