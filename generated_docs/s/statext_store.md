# statext_store

## Location
[src/backend/statistics/extended_stats.c:762-831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L762-L831)

## Overview
Serializes computed extended statistics data and stores it into the pg_statistic_ext_data system catalog table, managing the persistent storage of ndistinct, dependencies, MCV, and expression statistics.

## Definition

```c
struct a new pg_statistic_ext_data tuple, replacing the calculated
	 * stats.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = statext_ndistinct_serialize(ndistinct);

		nulls[Anum_pg_statistic_ext_data_stxdndistinct - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxdndistinct - 1] = PointerGetDatum(data);
	}

	if (dependencies != NULL)
	{
		bytea	   *data = statext_dependencies_serialize(dependencies);

		nulls[Anum_pg_statistic_ext_data_stxddependencies - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxddependencies - 1] = PointerGetDatum(data);
	}
	if (mcv != NULL)
	{
		bytea	   *data = statext_mcv_serialize(mcv, stats);

		nulls[Anum_pg_statistic_ext_data_stxdmcv - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_data_stxdmcv - 1] = PointerGetDatum(data);
	}
	if (exprs != (Datum) 0)
	{
		nulls[Anum_pg_statistic_ext_data_stxdexpr - 1] = false;
		values[Anum_pg_statistic_ext_data_stxdexpr - 1] = exprs;
	}

	/*
	 * Delete the old tuple if it exists, and insert a new one. It's easier
	 * than trying to update or insert, based on various conditions.
	 */
	RemoveStatisticsDataById(statOid, inh);
```
## Detailed Description
The statext_store function is responsible for the final step in PostgreSQL's extended statistics computation process - persisting the calculated statistical data to the system catalog. It handles the serialization and storage of various types of extended statistics that have been computed for a statistics object.

The function performs several key operations:
1. **Catalog Preparation**: Opens the pg_statistic_ext_data relation with exclusive write access
2. **Data Serialization**: Converts in-memory statistics structures (ndistinct, dependencies, MCV lists) into bytea format for storage
3. **Tuple Construction**: Builds a new heap tuple with all the serialized statistics data
4. **Atomic Update**: Removes any existing statistics data for the object and inserts the new tuple

The function implements a "delete-then-insert" strategy rather than update-or-insert, which simplifies the logic and ensures clean replacement of existing statistics. Each type of statistic (ndistinct, dependencies, MCV, expressions) is serialized using specialized functions and stored in separate columns of the catalog table.

## Parameters / Member Variables
- : The OID of the extended statistics object being stored
- : Boolean flag indicating whether these are inheritance statistics
- : Multi-variate n-distinct statistics data (can be NULL)
- : Functional dependencies statistics data (can be NULL)
- : Most Common Values list statistics data (can be NULL)
- : Serialized expression statistics data (can be 0/NULL)
- : Array of VacAttrStats used for MCV serialization context

## Dependencies
- Functions called/Symbols referenced:
  - [MVNDistinct](../M/MVNDistinct.md), MVDependencies, MCVList (statistics data structures)
  - [statext_ndistinct_serialize](statext_ndistinct_serialize.md), statext_dependencies_serialize, statext_mcv_serialize (serialization functions)
  - [table_open](../t/table_open.md), table_close (catalog access)
  - [RemoveStatisticsDataById](../R/RemoveStatisticsDataById.md) (existing data cleanup)
  - [heap_form_tuple](../h/heap_form_tuple.md), heap_freetuple (tuple management)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md) (catalog insertion)
  - [PointerGetDatum](../P/PointerGetDatum.md), ObjectIdGetDatum, BoolGetDatum (datum conversion)
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (main extended statistics builder)

## Notes and Other Information
- Uses RowExclusiveLock to ensure atomic updates to the statistics catalog
- Implements null handling for optional statistics types - only stores non-NULL data
- The delete-then-insert approach avoids complex conditional logic for updates vs. inserts
- Each statistics type is serialized independently, allowing for partial statistics storage
- Expression statistics are handled differently as they're already in Datum format
- Memory management includes proper cleanup of formed tuples
- The function is called only after all extended statistics have been successfully computed
- Storage format uses PostgreSQL's standard bytea serialization for complex data structures
- The inheritance flag allows for separate storage of regular and inheritance-based statistics