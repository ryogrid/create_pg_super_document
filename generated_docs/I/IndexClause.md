# IndexClause

## Location
[src/include/nodes/pathnodes.h:1755-1765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1755-L1765)

## Overview
IndexClause is a structure that represents how a WHERE or JOIN clause can be applied to a particular index during query planning, including both directly-usable and transformed index conditions.

## Definition

```c
typedef struct IndexClause
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;
	struct RestrictInfo *rinfo; /* original restriction or join clause */
	List	   *indexquals;		/* indexqual(s) derived from it */
	bool		lossy;			/* are indexquals a lossy version of clause? */
	AttrNumber	indexcol;		/* index column the clause uses (zero-based) */
	List	   *indexcols;		/* multiple index columns, if RowCompare */
} IndexClause;
```
## Detailed Description
IndexClause represents the mapping between query conditions and index usage during query planning. Each IndexClause references a RestrictInfo node from the query's WHERE or JOIN conditions and shows how that restriction can be applied to a particular index.

The structure supports both directly-usable indexclauses (typically of the form "indexcol OP pseudoconstant") and those requiring transformation. Common transformations include:
- Commuting clauses like "pseudoconstant OP indexcol" to "indexcol OP pseudoconstant"
- Extracting indexable range conditions from LIKE patterns (e.g., "x LIKE 'foo%'" becomes "x >= 'foo' AND x < 'fop'")
- Deriving conditions through planner support functions attached to operators

The indexquals list contains RestrictInfos for directly-usable index conditions. In simple cases, it's a single-element list containing the original rinfo. For transformed conditions, it may contain multiple derived indexqual conditions. The lossy flag indicates whether these conditions are semantically equivalent to the original or represent a weaker condition.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : Pointer to the original RestrictInfo representing the WHERE/JOIN clause
- : List of RestrictInfo nodes for directly-usable index conditions derived from the original clause
- : Boolean flag indicating whether indexquals represent a lossy (weaker) version of the original clause
- : Zero-based index of the primary index column used by this clause
- : List of all affected index columns for RowCompareExpr clauses (NIL for single-column clauses)

## Dependencies
- Functions called/Symbols referenced:
  - [RestrictInfo](../R/RestrictInfo.md) (referenced structure)
  - [List](../L/List.md) (PostgreSQL list structure)
  - NodeTag (node type identification)
  - AttrNumber (attribute number type)

- Called from (representative examples):
  - [build_index_paths](../b/build_index_paths.md) (path generation)
  - [match_clause_to_index](../m/match_clause_to_index.md) (clause matching)
  - [create_bitmap_subplan](../c/create_bitmap_subplan.md) (bitmap scan planning)
  - [get_quals_from_indexclauses](../g/get_quals_from_indexclauses.md) (selectivity estimation)
  - [btcostestimate](../b/btcostestimate.md) (B-tree cost estimation)

## Notes and Other Information
- [IndexClause](IndexClause.md) lists in IndexPath must be ordered by index column (indexcol values in nondecreasing sequence)
- Multiple clauses for the same index column can appear in any order
- The structure supports complex multi-column operations through the indexcols field for RowCompareExpr
- Transformation of conditions into indexable forms is handled by planner support functions
- The lossy flag is crucial for cost estimation and execution planning as it affects result accuracy