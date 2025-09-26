# CreateStatsStmt

## Location
[src/include/nodes/parsenodes.h:3384-3394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3384-L3394)

## Overview
CreateStatsStmt represents a parsed CREATE STATISTICS statement, encapsulating all information needed to create extended statistics objects on table columns or expressions.

## Definition

```c
typedef struct CreateStatsStmt
{
	NodeTag		type;
	List	   *defnames;		/* qualified name (list of String) */
	List	   *stat_types;		/* stat types (list of String) */
	List	   *exprs;			/* expressions to build statistics on */
	List	   *relations;		/* rels to build stats on (list of RangeVar) */
	char	   *stxcomment;		/* comment to apply to stats, or NULL */
	bool		transformed;	/* true when transformStatsStmt is finished */
	bool		if_not_exists;	/* do nothing if stats name already exists */
} CreateStatsStmt;
```
## Detailed Description
CreateStatsStmt is a parse tree node that represents the SQL CREATE STATISTICS command. It contains all the parsed information from the SQL statement including the statistics name, types of statistics to collect, target columns/expressions, and associated tables. The structure is used during the parsing and transformation phases before the actual statistics object is created in the system catalogs.

The node supports creating multi-column statistics (like multi-variate correlation statistics) and can handle both simple column references and complex expressions. The transformed flag indicates whether the statement has gone through semantic analysis and transformation.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL parse tree nodes
- : Qualified name of the statistics object as a list of strings (schema.stats_name)
- : List of statistics types to collect (e.g., 'ndistinct', 'dependencies', 'mcv')
- : List of column references or expressions to build statistics on
- : List of RangeVar nodes representing the tables to analyze
- : Optional comment string to associate with the statistics object
- : Boolean flag indicating completion of semantic transformation
- : Boolean flag for IF NOT EXISTS clause handling

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for node creation)
  - NodeTag (parse tree infrastructure)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [RangeVar](../R/RangeVar.md) (table reference structure)

- Called from (representative examples):
  - [transformStatsStmt](../t/transformStatsStmt.md) (parse transformation)
  - [generateClonedExtStatsStmt](../g/generateClonedExtStatsStmt.md) (table cloning)
  - [CreateStatistics](CreateStatistics.md) (statistics creation command)
  - [ATExecAddStatistics](../A/ATExecAddStatistics.md) (ALTER TABLE ADD STATISTICS)

## Notes and Other Information
- Part of PostgreSQL's extended statistics framework introduced for multi-column statistics
- Used in CREATE STATISTICS SQL command processing
- The transformed field helps distinguish between raw parsed statements and semantically analyzed ones
- Supports the IF NOT EXISTS syntax to avoid errors when statistics already exist
- Can be generated programmatically during table operations like LIKE clause processing