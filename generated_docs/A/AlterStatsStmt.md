# AlterStatsStmt

## Location
[src/include/nodes/parsenodes.h:3415-3421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3415-L3421)

## Overview
AlterStatsStmt represents a parsed ALTER STATISTICS statement, used to modify properties of existing extended statistics objects, primarily their statistics target values.

## Definition

```c
typedef struct AlterStatsStmt
{
	NodeTag		type;
	List	   *defnames;		/* qualified name (list of String) */
	Node	   *stxstattarget;	/* statistics target */
	bool		missing_ok;		/* skip error if statistics object is missing */
} AlterStatsStmt;
```
## Detailed Description
AlterStatsStmt is a parse tree node that represents the SQL ALTER STATISTICS command. It is used to modify existing extended statistics objects, particularly to change their statistics target values which control how much sampling effort PostgreSQL puts into collecting statistics for that object. The statistics target affects the accuracy and cost of statistics collection.

The statement can optionally use IF EXISTS semantics through the missing_ok flag, allowing scripts to alter statistics objects without failing when they don't exist.

## Parameters / Member Variables
- `type`: Standard NodeTag for PostgreSQL parse tree nodes
- `*defnames`: Qualified name of the statistics object as a list of strings (schema.stats_name)
- `*stxstattarget`: Node representing the new statistics target value (typically an integer constant)
- `missing_ok`: Boolean flag for IF EXISTS clause - true to skip errors when statistics object doesn't exist
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree infrastructure)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [Node](../N/Node.md) (base parse tree node type)

- Called from (representative examples):
  - [AlterStatistics](AlterStatistics.md) (main ALTER STATISTICS command handler)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command dispatcher)

## Notes and Other Information
- Used primarily for changing statistics target values on extended statistics objects
- Statistics target controls the trade-off between statistics accuracy and collection cost
- The missing_ok flag implements IF EXISTS semantics for safer scripting
- Part of PostgreSQL's extended statistics management commands
- Less commonly used compared to CREATE STATISTICS and DROP STATISTICS
- The stxstattarget typically contains an integer value between -1 and 10000