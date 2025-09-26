# LimitOption

## Location
[src/include/nodes/nodes.h:432-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodes.h#L432-L434)

## Overview
LimitOption is an enumeration that defines the type of LIMIT clause behavior for query result limiting in PostgreSQL's query execution engine.

## Definition

```c
typedef enum LimitOption
{
	LIMIT_OPTION_COUNT,			/* FETCH FIRST... ONLY */
	LIMIT_OPTION_WITH_TIES,		/* FETCH FIRST... WITH TIES */
} LimitOption;
```
## Detailed Description
LimitOption specifies the semantics of LIMIT operations in PostgreSQL queries, particularly for FETCH FIRST clauses. It distinguishes between standard row count limiting and the WITH TIES variant that includes additional rows when they have the same values as the last row in the result set. This enumeration is used by the query planner and executor to determine the appropriate behavior for result set limiting. The enum is defined in nodes.h because it's needed in both parsenodes.h and plannodes.h for parsing and plan node execution.

## Parameters / Member Variables
- : Standard FETCH FIRST... ONLY behavior that returns exactly the specified number of rows
- : FETCH FIRST... WITH TIES behavior that returns the specified number of rows plus any additional rows that have the same values as the last row in the ordered result set

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no function calls)
- Called from (representative examples):
  - [make_limit](../m/make_limit.md) (src/backend/optimizer/plan/createplan.c:6962)
  - [create_limit_path](../c/create_limit_path.md) (src/backend/optimizer/util/pathnode.c:3829)
  - [transformLimitClause](../t/transformLimitClause.md) (src/backend/parser/parse_clause.c:1883)
  - [LimitState](LimitState.md) (src/include/nodes/execnodes.h:2841)
  - [Query](../Q/Query.md) (src/include/nodes/parsenodes.h:215)
  - [SelectStmt](../S/SelectStmt.md) (src/include/nodes/parsenodes.h:2151)
  - [LimitPath](LimitPath.md) (src/include/nodes/pathnodes.h:2406)
  - [Limit](Limit.md) (src/include/nodes/plannodes.h:1281)

## Notes and Other Information
This enumeration is particularly important for implementing SQL standard FETCH FIRST clauses with the WITH TIES option. The WITH TIES functionality requires the query to have an ORDER BY clause, as it needs to determine which rows have equivalent values to include the 'tied' rows. The placement in nodes.h reflects its fundamental role in query parsing, planning, and execution infrastructure.