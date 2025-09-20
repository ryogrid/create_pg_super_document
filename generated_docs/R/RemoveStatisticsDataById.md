# RemoveStatisticsDataById

## Location
[src/backend/commands/statscmds.c:722-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/statscmds.c#L722-L746)

## Overview
Deletes an entry from the pg_statistic_ext_data catalog table for a specific statistics object and inheritance flag combination.

## Definition

```c
void
RemoveStatisticsDataById(Oid statsOid, bool inh)
```
## Detailed Description
This utility function removes statistical data from the pg_statistic_ext_data system catalog for a given extended statistics object. The function is designed to be tolerant of missing data - it searches for the specified row but does not error if the row doesn't exist, making it safe to call during cleanup operations.

The pg_statistic_ext_data table stores the actual computed statistical data (like ndistinct values, dependencies, MCV lists) for extended statistics objects, separate from the metadata stored in pg_statistic_ext. This separation allows the metadata to persist while the data can be removed/regenerated during ANALYZE operations.

The function takes both a statistics object OID and an inheritance flag to handle the distinction between statistics computed for a table directly versus statistics that include data from inheritance children.

## Parameters / Member Variables
- : OID of the extended statistics object whose data should be removed
- : Boolean flag indicating whether to remove inherited statistics data (true) or direct table statistics data (false)

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens StatisticExtDataRelationId with RowExclusiveLock)
  - [SearchSysCache2](../S/SearchSysCache2.md) (looks up row by STATEXTDATASTXOID cache)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (removes the tuple if found)
  - table_close (closes the catalog relation)
- Called from (representative examples):
  - [RemoveStatisticsById](RemoveStatisticsById.md) (src/backend/commands/statscmds.c:777-778)
  - [statext_store](../s/statext_store.md) (src/backend/statistics/extended_stats.c:819)

## Notes and Other Information
- Does not error if the target row doesn't exist, making it safe for cleanup operations
- Requires RowExclusiveLock on StatisticExtDataRelationId to ensure exclusive access during deletion
- The inheritance flag distinguishes between statistics computed on the table alone vs. including inheritance children
- Called during statistics object deletion and when regenerating statistics data during ANALYZE
- Part of PostgreSQL's extended statistics infrastructure for multivariate statistics