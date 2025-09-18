# CreateStatistics

## Location
src/backend/commands/statscmds.c: 62 - 598

## Overview
Creates a new PostgreSQL extended statistics object that tracks column correlations, functional dependencies, and other multivariate statistics to improve query planner estimates.

## Definition


## Detailed Description
This function implements the CREATE STATISTICS SQL command, which creates extended statistics objects on table columns and expressions. Extended statistics help the query planner make better estimates for queries involving multiple correlated columns by tracking relationships like functional dependencies, n-distinct counts, and most common value lists across column combinations.

The function performs extensive validation including checking relation permissions, column existence, data type compatibility, and duplicate detection. It supports statistics on regular columns, expressions, or a combination of both. The created statistics object is stored in the pg_statistic_ext system catalog with appropriate dependency tracking.

Key features include:
- Support for multiple statistics types (ndistinct, dependencies, mcv, expressions)
- Validation of column references and expressions  
- Automatic name generation when not specified
- Comprehensive dependency tracking for columns, expressions, namespace, and owner
- Integration with PostgreSQL's object management system

## Parameters / Member Variables
- : CreateStatsStmt structure containing the parsed CREATE STATISTICS command with relation names, column/expression lists, statistics types, and optional name/comment

## Dependencies
- Functions called/Symbols referenced:
  - relation_openrv (opens the target relation)
  - ChooseExtendedStatisticName (generates automatic names)
  - SearchSysCacheAttName (validates column references)
  - compare_int16 (sorts column attribute numbers)
  - buildint2vector (creates column number array)
  - CatalogTupleInsert (inserts into pg_statistic_ext)
  - recordDependencyOn (tracks object dependencies)
  - CreateComments (adds optional comments)
- Called from (representative examples):
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1902)
  - ATExecAddStatistics (src/backend/commands/tablecmds.c:9252)

## Notes and Other Information
- Supports statistics on regular tables, materialized views, foreign tables, and partitioned tables
- Requires ShareUpdateExclusiveLock on the target relation to avoid conflicts with concurrent ANALYZE
- Maximum of STATS_MAX_DIMENSIONS (32) columns allowed per statistics object
- System columns cannot be included in extended statistics
- Data types must have a default btree operator class (less-than operator)
- Creates automatic dependencies so statistics are dropped when referenced columns are dropped
- Statistics objects are not considered extension members (no ALTER EXTENSION support)
- Returns InvalidObjectAddress if IF NOT EXISTS is specified and object already exists