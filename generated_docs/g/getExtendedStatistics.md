# getExtendedStatistics

## Location
[src/bin/pg_dump/pg_dump.c:7743-7821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7743-L7821)

## Overview
Retrieves information about extended statistics objects from the PostgreSQL system catalog and creates corresponding DumpableObject entries for pg_dump operations.

## Definition

```c
void
getExtendedStatistics(Archive *fout)
```
## Detailed Description
The getExtendedStatistics function queries the pg_statistic_ext system catalog to retrieve all extended statistics objects in the database. Extended statistics provide multi-column statistics that help the query planner make better estimates for complex predicates involving multiple columns. The function handles version-specific differences, as extended statistics were introduced in PostgreSQL 10 and the stxstattarget column was added in PostgreSQL 13. For each extended statistics object found, it creates a StatsExtInfo structure containing metadata such as the statistics name, namespace, owner, target table, and statistics target value. The function also determines whether each statistics object should be included in the dump based on the current dump configuration.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and database connection information

## Dependencies
- Functions called/Symbols referenced:
  - [StatsExtInfo](../S/StatsExtInfo.md) (structure type)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - pg_malloc
  - DO_STATSEXT (enum value)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [findTableByOid](../f/findTableByOid.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [selectDumpableStatisticsObject](../s/selectDumpableStatisticsObject.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only available for PostgreSQL 10.0000 and later (extended statistics introduction)
- Handles version-specific SQL queries for stxstattarget column availability (13.0000+)
- Creates DumpableObject entries that are registered in the global dump object system
- The stattarget field represents the statistics collection target, with -1 indicating default behavior
- Extended statistics objects include functional dependencies, n-distinct coefficients, and MCV (Most Common Values) lists
- The selectDumpableStatisticsObject function determines dump eligibility based on table and schema inclusion rules
- Memory allocation is handled automatically based on the number of statistics objects found
- Statistics objects are associated with specific tables through the stxrelid foreign key relationship