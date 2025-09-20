# CTEMaterialize

## Location
[src/include/nodes/parsenodes.h:1641-1642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1641-L1642)

## Overview
CTEMaterialize is an enumeration that controls the materialization behavior of Common Table Expressions (CTEs) in PostgreSQL, allowing users to specify whether a CTE should be materialized, not materialized, or use the default behavior.

## Definition

```c
typedef struct CTESearchClause
{
	NodeTag		type;
	List	   *search_col_list;
	bool		search_breadth_first;
	char	   *search_seq_column;
	ParseLoc	location;
} CTESearchClause;
```
## Detailed Description
CTEMaterialize defines the materialization strategy for Common Table Expressions (WITH clauses). Materialization determines whether PostgreSQL should compute and store the CTE results in memory before using them, or whether it should inline the CTE query directly into the main query. This choice significantly affects query performance and execution behavior. The enumeration provides explicit control over this optimization decision, overriding PostgreSQL's default heuristics.

## Parameters / Member Variables
- : Uses PostgreSQL's default materialization heuristics (no explicit MATERIALIZED or NOT MATERIALIZED clause)
- : Forces materialization of the CTE (MATERIALIZED clause specified)
- : Prevents materialization, inlining the CTE instead (NOT MATERIALIZED clause specified)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - CommonTableExpr (src/include/nodes/parsenodes.h:1679)

## Notes and Other Information
- [CTEMaterialize](CTEMaterialize.md) acts as an optimization fence when set to CTEMaterializeAlways
- Materialization can prevent multiple evaluations of expensive CTE queries
- NOT MATERIALIZED allows the query planner to inline and optimize the CTE more aggressively
- The default behavior depends on PostgreSQL's cost-based optimization decisions
- This feature was introduced to give users explicit control over CTE evaluation strategy
- Materialization choice affects query plan shape and can impact performance significantly
- The enumeration is used in the CommonTableExpr structure as the 'ctematerialized' field