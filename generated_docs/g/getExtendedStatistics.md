# getExtendedStatistics

## Location
src/bin/pg_dump/pg_dump.c: 7743 - 7821

## Overview
Retrieves information about extended statistics objects from the PostgreSQL system catalog and creates corresponding DumpableObject entries for pg_dump operations.

## Definition


## Detailed Description
The getExtendedStatistics function queries the pg_statistic_ext system catalog to retrieve all extended statistics objects in the database. Extended statistics provide multi-column statistics that help the query planner make better estimates for complex predicates involving multiple columns. The function handles version-specific differences, as extended statistics were introduced in PostgreSQL 10 and the stxstattarget column was added in PostgreSQL 13. For each extended statistics object found, it creates a StatsExtInfo structure containing metadata such as the statistics name, namespace, owner, target table, and statistics target value. The function also determines whether each statistics object should be included in the dump based on the current dump configuration.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and database connection information

## Dependencies
- Functions called/Symbols referenced:
  - StatsExtInfo (structure type)
  - ExecuteSqlQuery
  - PGRES_TUPLES_OK (constant)
  - pg_malloc
  - DO_STATSEXT (enum value)
  - atooid
  - AssignDumpId
  - findNamespace
  - getRoleName
  - findTableByOid
  - PQgetisnull
  - selectDumpableStatisticsObject
- Called from (representative examples):
  - getSchemaData
  - SubRelInfo (referenced in header)

## Notes and Other Information
- Only available for PostgreSQL 10.0000 and later (extended statistics introduction)
- Handles version-specific SQL queries for stxstattarget column availability (13.0000+)
- Creates DumpableObject entries that are registered in the global dump object system
- The stattarget field represents the statistics collection target, with -1 indicating default behavior
- Extended statistics objects include functional dependencies, n-distinct coefficients, and MCV (Most Common Values) lists
- The selectDumpableStatisticsObject function determines dump eligibility based on table and schema inclusion rules
- Memory allocation is handled automatically based on the number of statistics objects found
- Statistics objects are associated with specific tables through the stxrelid foreign key relationship