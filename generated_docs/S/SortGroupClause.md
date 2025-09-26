# SortGroupClause

## Location
[src/include/nodes/parsenodes.h:1436-1445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1436-L1445)

## Overview
SortGroupClause represents ordering and grouping specifications for ORDER BY, GROUP BY, PARTITION BY, DISTINCT, and DISTINCT ON clauses, providing a unified representation to enable efficient query planning when multiple operations require similar sorting or grouping.

## Definition

```c
typedef struct SortGroupClause
{
	NodeTag		type;
	Index		tleSortGroupRef;	/* reference into targetlist */
	Oid			eqop;			/* the equality operator ('=' op) */
	Oid			sortop;			/* the ordering operator ('<' op), or 0 */
	bool		nulls_first;	/* do NULLs come before normal values? */
	/* can eqop be implemented by hashing? */
	bool		hashable pg_node_attr(query_jumble_ignore);
} SortGroupClause;
```
## Detailed Description
SortGroupClause provides a unified representation for ordering and equality operations across different SQL clauses (ORDER BY, GROUP BY, PARTITION BY, DISTINCT, DISTINCT ON). This design enables the optimizer to recognize when grouping operations can reuse sorting work, or when a single sort operation can satisfy both grouping and ordering requirements.

The structure maintains both equality and ordering operators because grouping can be implemented through sorting followed by duplicate elimination. By tracking both operators, PostgreSQL can optimize cases where ORDER BY and GROUP BY operations can share the same sort step.

For ORDER BY items, all fields must be valid (though collation information is obtained from the referenced targetlist expression). For grouping items, the eqop must be valid, and if it's a btree equality operator, sortop should contain a compatible ordering operator. For hash-only datatypes, sortop remains InvalidOid and the item can only use hash-based grouping.

## Parameters / Member Variables
- : NodeTag identifying this as a SortGroupClause node
- : Index referencing the targetlist entry that contains the expression to be sorted or grouped
- : OID of the equality operator used for grouping comparisons
- : OID of the ordering operator for sorting, or InvalidOid if not available/applicable
- : Boolean indicating whether NULL values should sort before non-NULL values
- : Boolean flag indicating whether the equality operator supports hash-based implementation

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - Index (for targetlist references)  
  - Oid (for operator references)
- Called from (representative examples):
  - transformGroupClauseExpr (parser/parse_clause.c)
  - make_pathkeys_for_sortclauses_extended (optimizer/path/pathkeys.c)
  - create_unique_plan (optimizer/plan/createplan.c)
  - preprocess_groupclause (optimizer/plan/planner.c)

## Notes and Other Information
- The parser may rearrange distinctClause lists to match ORDER BY requirements, ensuring only one sort step is needed
- For collation-sensitive sorting, collation information must be obtained from the referenced targetlist expression
- The hashable flag is precomputed during construction to avoid expensive recalculation later
- When both ORDER BY and grouping are present, PostgreSQL attempts to choose operators that satisfy both requirements efficiently
- The design enables sophisticated optimizations like removing redundant sorts when grouping and ordering requirements align