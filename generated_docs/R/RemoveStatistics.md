# RemoveStatistics

## Location
[src/backend/catalog/heap.c:2974-3020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2974-L3020)

## Overview
RemoveStatistics removes statistics entries from the pg_statistic system catalog table for either a specific column or all columns of a relation.

## Definition
void RemoveStatistics(Oid relid, AttrNumber attnum)

## Detailed Description
This function removes statistics information stored in PostgreSQL's pg_statistic system catalog. It can operate in two modes: if attnum is zero, it removes all statistics entries for the specified relation; otherwise, it removes only the statistics entries for the specified column. The function handles inherited statistics by looping through all matching entries, even when targeting a specific column.

The function opens the pg_statistic table with RowExclusiveLock, sets up scan keys based on the parameters, performs a system catalog scan, and deletes all matching tuples. This is typically called during DROP operations or when column types are altered, requiring statistics to be regenerated.

## Parameters / Member Variables
- : Object identifier of the relation whose statistics should be removed
- : Attribute number of the specific column (0 means remove statistics for all columns of the relation)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int16GetDatum](../I/Int16GetDatum.md)
  - HeapTupleIsValid
- Called from (representative examples):
  - [RemoveAttributeById](RemoveAttributeById.md)
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [index_drop](../i/index_drop.md)
  - [ATExecSetExpression](../A/ATExecSetExpression.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)

## Notes and Other Information
- The function must loop through all matching entries even when attnum != 0 because of inherited statistics from parent tables
- Uses RowExclusiveLock on pg_statistic to ensure exclusive access during deletion
- Statistics removal is typically followed by regeneration via ANALYZE or automatic statistics collection
- The function is part of PostgreSQL's metadata management system for maintaining data distribution statistics used by the query planner