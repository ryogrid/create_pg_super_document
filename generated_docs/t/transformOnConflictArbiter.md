# transformOnConflictArbiter

## Location
[src/backend/parser/parse_clause.c:3297-3392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L3297-L3392)

## Overview
Transforms ON CONFLICT arbiter specifications into the internal representation used by PostgreSQL's conflict resolution system, handling index inference expressions, WHERE clauses, and constraint name resolution.

## Definition

```c
void
transformOnConflictArbiter(ParseState *pstate,
						   OnConflictClause *onConflictClause,
						   List **arbiterExpr, Node **arbiterWhere,
						   Oid *constraint)
```
## Detailed Description
This function is the main entry point for transforming ON CONFLICT clauses in INSERT statements with UPSERT functionality. It processes the arbiter specification that tells PostgreSQL which unique constraint or index should be used to detect conflicts. The function handles three main forms of arbiters: explicit column/expression lists, constraint names, and WHERE clauses for partial unique indexes.

The function performs several critical validations, including rejecting ON CONFLICT on system catalog tables and tables used as catalog tables by logical decoding. For ON CONFLICT DO UPDATE, it enforces that an inference specification or constraint name must be provided. When processing inference specifications, it delegates expression transformation to resolve_unique_index_expr and handles WHERE clauses for partial unique index inference.

When a constraint name is specified, the function resolves it to get the constraint OID and marks the constrained columns as requiring SELECT privilege, ensuring proper permission checking. The transformed results are returned through output parameters for use by the planner.

## Parameters / Member Variables
- : Parse state context containing parsing information and target relation details
- : The parsed ON CONFLICT clause containing the arbiter specification
- : Output parameter for the list of transformed arbiter expressions
- : Output parameter for the transformed WHERE clause (for partial indexes)
- : Output parameter for the constraint OID when specified by name

## Dependencies
- Functions called/Symbols referenced:
  - [resolve_unique_index_expr](../r/resolve_unique_index_expr.md): Transforms index element expressions for unique index inference
  - [transformExpr](transformExpr.md): Transforms WHERE clause expressions using index predicate expression kind
  - [get_relation_constraint_attnos](../g/get_relation_constraint_attnos.md): Resolves constraint names and returns constrained column numbers
  - [IsCatalogRelation](../I/IsCatalogRelation.md): Checks if a relation is a system catalog table
  - RelationIsUsedAsCatalogTable: Checks if a relation is used as a catalog table by logical decoding
  - [bms_add_members](../b/bms_add_members.md): Adds column numbers to permission bitmaps for access control
  - [exprLocation](../e/exprLocation.md): Gets parse locations for error reporting
  - OnConflictClause, InferClause: Node structures for ON CONFLICT processing
  - ONCONFLICT_UPDATE: Constant for UPDATE action type
  - EXPR_KIND_INDEX_PREDICATE: Expression kind for index predicate transformation
  - ACL_SELECT: Permission constant for SELECT access
- Called from (representative examples):
  - [transformOnConflictClause](transformOnConflictClause.md): Higher-level ON CONFLICT transformation in analyzer

## Notes and Other Information
- The function returns results through output parameters rather than return values
- Explicitly disallows ON CONFLICT on system catalog tables and tables used for logical decoding catalog purposes
- ON CONFLICT DO NOTHING does not require an inference specification, but DO UPDATE does
- When using constraint names, automatically marks the constrained columns for SELECT privilege to ensure proper permission checking
- Supports partial unique index inference through WHERE clause transformation using EXPR_KIND_INDEX_PREDICATE
- The transformation reuses CREATE INDEX infrastructure but adapts it specifically for conflict resolution purposes
- After transformation, uses dedicated primnode representation for inference elements that works with the query collation assignment system