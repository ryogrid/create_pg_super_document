# analyze_rel

## Location
[src/backend/commands/analyze.c:111-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L111-L279)

## Overview
The main entry point function for analyzing a single relation (table, materialized view, or foreign table) to gather statistics for the query planner.

## Definition


## Detailed Description
This function orchestrates the analysis of a single relation by performing several validation checks and then delegating to  for the actual statistics gathering. It handles different relation types (regular tables, materialized views, foreign tables, and partitioned tables) and implements proper locking mechanisms to prevent concurrent ANALYZE operations. For partitioned tables, it performs both non-recursive analysis (skipped for partitioned tables as they contain no data) and recursive analysis of child partitions when applicable.

The function performs comprehensive validation including privilege checks, relation type verification, and handles special cases like temporary tables of other backends and the system statistics table .

## Parameters / Member Variables
- : OID of the relation to analyze
- : RangeVar containing relation name information for error reporting (may be stale)
- : Vacuum parameters structure containing analysis options and configuration
- : List of specific columns to analyze (NULL for all columns)
- : Boolean indicating if running within an outer transaction
- : Buffer access strategy for controlling buffer replacement during analysis

## Dependencies
- Functions called/Symbols referenced:
  - : Opens and locks the relation for analysis
  - : Checks user privileges for analysis
  - : Closes the relation and manages locks
  - : Performs the actual statistics collection
  - /: Progress reporting
  - : Standard row sampling function for regular tables
  - : Foreign table analysis support
- Called from (representative examples):
  - : Main vacuum command entry point

## Notes and Other Information
- Uses ShareUpdateExclusiveLock to prevent concurrent ANALYZE operations on the same relation
- Skips analysis for temporary tables of other backends and the pg_statistic system table
- Supports foreign table analysis through FDW-specific hooks
- For partitioned tables, performs recursive analysis of child partitions when  is true
- Maintains locks until transaction commit to ensure consistency of statistics updates