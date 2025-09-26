# VacuumStmt

## Location
src/include/nodes/parsenodes.h: 3837 - 3843

## Overview
A parse node structure representing both VACUUM and ANALYZE statements, used to reclaim storage and update table statistics.

## Definition


## Detailed Description
VacuumStmt is a parse node structure that represents both VACUUM and ANALYZE SQL statements. This unified structure handles commands like "VACUUM table_name", "ANALYZE table_name", "VACUUM (FULL, VERBOSE) table_name", or "VACUUM;" (all tables). VACUUM reclaims storage occupied by dead tuples and can defragment tables, while ANALYZE collects statistics about data distribution for the query planner.

The structure supports extensive options for controlling vacuum behavior including parallelism, buffer usage limits, index cleanup strategies, and selective processing of main tables versus TOAST tables. Both operations can be combined in a single command for efficiency.

## Parameters / Member Variables
- : NodeTag identifying this as a VacuumStmt node
- : List of DefElem nodes containing command options (FULL, VERBOSE, ANALYZE, FREEZE, etc.)
- : List of VacuumRelation structures specifying target tables, or NIL to process all tables
- : Boolean flag distinguishing VACUUM (true) from ANALYZE (false) commands

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL list structure for options and relations)
  - VacuumRelation (for specifying target tables and columns)
  - DefElem (for option definitions)
- Called from (representative examples):
  - ExecVacuum (execution function in vacuum.c)
  - standard_ProcessUtility (utility command processing)
  - CreateCommandTag (for command logging)

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