# StatsExtInfo

## Location
src/bin/pg_dump/pg_dump.h: 440 - 441

## Overview
StatsExtInfo represents extended statistics objects in PostgreSQL's pg_dump utility, containing metadata about multivariate statistics and their associated tables and ownership.

## Definition


## Detailed Description
StatsExtInfo is a data structure in pg_dump that represents extended statistics objects (also known as multivariate statistics) in PostgreSQL databases. Extended statistics allow the query planner to make better estimates for queries involving multiple columns by collecting cross-column statistical information that goes beyond the single-column statistics maintained in pg_statistic.

This structure captures the essential metadata needed to recreate extended statistics objects during database restoration, including ownership information, the associated table, and the statistics target (sample size) used for data collection. Extended statistics can include functional dependencies, n-distinct values, and most common value lists across multiple columns.

The structure is part of PostgreSQL's advanced query optimization infrastructure, introduced to improve query planning for complex WHERE clauses involving multiple correlated columns.

## Parameters / Member Variables
- : Base DumpableObject containing common metadata like catalog ID, dump ID, name, namespace, and dependencies
- : Name of the role (user) that owns the extended statistics object
- : Pointer to the TableInfo structure representing the table for which these statistics are collected
- : Statistics target value controlling the amount of data sampled for statistics collection (similar to ALTER TABLE ... ALTER COLUMN ... SET STATISTICS)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (inherited base structure)
  - TableInfo (referenced via stattable pointer)
- Called from (representative examples):
  - getExtendedStatistics (creates and populates StatsExtInfo structures)
  - dumpStatisticsExt (generates CREATE STATISTICS commands)
  - selectDumpableStatisticsObject (determines if statistics should be dumped)
  - dumpDumpableObject (generic dump processing)

## Notes and Other Information
- StatsExtInfo objects correspond to CREATE STATISTICS commands in PostgreSQL
- Extended statistics help the query planner with complex multi-column WHERE clauses and JOINs
- The stattarget value controls how much data is sampled when ANALYZE updates the statistics
- These statistics are particularly useful for detecting functional dependencies and cross-column correlations
- Introduced as part of PostgreSQL's multivariate statistics feature (version 10+)
- The structure ensures proper ownership restoration through the rolname field
- Extended statistics objects are schema-level objects and follow standard dependency rules
- Statistics collection is triggered by ANALYZE and uses the stattarget to determine sample size