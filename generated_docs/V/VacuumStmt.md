# VacuumStmt

## Location
[src/include/nodes/parsenodes.h:3837-3843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3837-L3843)

## Overview
A parse node structure representing both VACUUM and ANALYZE statements, used to reclaim storage and update table statistics.

## Definition

```c
typedef struct VacuumStmt
{
	NodeTag		type;
	List	   *options;		/* list of DefElem nodes */
	List	   *rels;			/* list of VacuumRelation, or NIL for all */
	bool		is_vacuumcmd;	/* true for VACUUM, false for ANALYZE */
} VacuumStmt;
```
## Detailed Description
VacuumStmt is a parse node structure that represents both VACUUM and ANALYZE SQL statements. This unified structure handles commands like "VACUUM table_name", "ANALYZE table_name", "VACUUM (FULL, VERBOSE) table_name", or "VACUUM;" (all tables). VACUUM reclaims storage occupied by dead tuples and can defragment tables, while ANALYZE collects statistics about data distribution for the query planner.

The structure supports extensive options for controlling vacuum behavior including parallelism, buffer usage limits, index cleanup strategies, and selective processing of main tables versus TOAST tables. Both operations can be combined in a single command for efficiency.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a VacuumStmt node
- `*options`: List of DefElem nodes containing command options (FULL, VERBOSE, ANALYZE, FREEZE, etc.)
- `*rels`: List of VacuumRelation structures specifying target tables, or NIL to process all tables
- `is_vacuumcmd`: Boolean flag distinguishing VACUUM (true) from ANALYZE (false) commands
## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list structure for options and relations)
  - [VacuumRelation](VacuumRelation.md) (for specifying target tables and columns)
  - [DefElem](../D/DefElem.md) (for option definitions)
- Called from (representative examples):
  - [ExecVacuum](../E/ExecVacuum.md) (execution function in vacuum.c)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)
  - [CreateCommandTag](../C/CreateCommandTag.md) (for command logging)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from Node via the NodeTag
- Supports numerous options: FULL, VERBOSE, ANALYZE, FREEZE, PARALLEL, BUFFER_USAGE_LIMIT, INDEX_CLEANUP, TRUNCATE, SKIP_LOCKED, etc.
- VACUUM FULL requires exclusive table locks and rewrites the entire table
- ANALYZE can target specific columns when specified in VacuumRelation.va_cols
- Parallel vacuum is supported for regular VACUUM but not VACUUM FULL
- Both operations maintain transaction MVCC consistency and can be interrupted safely
- VACUUM is essential for preventing transaction ID wraparound in long-running systems
- ANALYZE updates pg_statistic for optimal query planning
- The command can process multiple tables but uses separate transactions to avoid long lock holds