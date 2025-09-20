# ParseNamespaceColumn

## Location
[src/include/parser/parse_node.h:319-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_node.h#L319-L331)

## Overview
ParseNamespaceColumn represents metadata about a single column in a ParseNamespaceItem, containing the information needed to construct a Var node referencing that column during SQL parsing and semantic analysis.

## Definition

```c
struct ParseNamespaceColumn
{
	Index		p_varno;		/* rangetable index */
	AttrNumber	p_varattno;		/* attribute number of the column */
	Oid			p_vartype;		/* pg_type OID */
	int32		p_vartypmod;	/* type modifier value */
	Oid			p_varcollid;	/* OID of collation, or InvalidOid */
	Index		p_varnosyn;		/* rangetable index of syntactic referent */
	AttrNumber	p_varattnosyn;	/* attribute number of syntactic referent */
	bool		p_dontexpand;	/* not included in star expansion */
};
```
## Detailed Description
ParseNamespaceColumn stores per-column metadata within a ParseNamespaceItem to enable proper variable construction during query parsing. This structure handles the complexity of JOIN columns where the semantic referent (the actual source column) may differ from the syntactic referent (how the column appears in the query).

For regular base relation columns, p_varno/p_varattno and p_varnosyn/p_varattnosyn are identical. However, for JOIN USING columns that aren't semantically equivalent to either input column (in FULL joins or when type coercion is required), the semantic referent points to the JOIN RTE itself, while the syntactic referent may point to an aliased JOIN that hides the semantic referent's name.

Dropped columns are represented with all-zero values, conventionally detected by testing p_varno == 0.

## Parameters / Member Variables
- `p_varno`: Range table index identifying the semantic referent relation
- `p_varattno`: Attribute number of the column in the semantic referent
- `p_vartype`: PostgreSQL type OID for the column's data type
- `p_vartypmod`: Type modifier providing additional type information
- `p_varcollid`: Collation OID for the column, or InvalidOid if not applicable
- `p_varnosyn`: Range table index of the syntactic referent (how column appears in query)
- `p_varattnosyn`: Attribute number in the syntactic referent
- `p_dontexpand`: Flag indicating this column should be excluded from star (*) expansion

## Dependencies
- Functions called/Symbols referenced:
  - Index (type from PostgreSQL catalog system)
  - AttrNumber (type from PostgreSQL catalog system)
  - Oid (type from PostgreSQL catalog system)
- Called from (representative examples):
  - [buildVarFromNSColumn](../b/buildVarFromNSColumn.md) (src/backend/parser/parse_clause.c:1640)
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md) (src/backend/parser/parse_relation.c:731)
  - [buildNSItemFromTupleDesc](../b/buildNSItemFromTupleDesc.md) (src/backend/parser/parse_relation.c:1299)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (src/backend/parser/parse_clause.c:1167)

## Notes and Other Information
- This structure is part of PostgreSQL's parser infrastructure for resolving column references in SQL queries
- The dual referent system (semantic vs syntactic) enables proper handling of complex JOIN scenarios
- Used extensively in FROM clause processing and variable construction
- Critical for maintaining correct column visibility and reference resolution in nested queries and joins