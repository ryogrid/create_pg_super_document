# statext_ndistinct_load

## Location
[src/backend/statistics/mvdistinct.c:148-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L148-L178)

## Overview
Loads ndistinct statistics from the system catalog pg_statistic_ext_data for a specified multivariate statistics object.

## Definition

```c
struct, plus one base struct
	 * for each item, including number of items for each.
	 */
	len = VARHDRSZ + SizeOfHeader;
```
## Detailed Description
This function retrieves previously computed and stored ndistinct statistics from the PostgreSQL system catalog. It performs a cache lookup in the pg_statistic_ext_data table using the statistics object OID and inheritance flag, then deserializes the stored binary data back into an MVNDistinct structure.

The function includes comprehensive error handling for missing statistics objects and cases where the requested ndistinct statistics have not yet been computed. The deserialization is handled by calling statext_ndistinct_deserialize on the retrieved binary data.

## Parameters / Member Variables
- : Object identifier (OID) of the multivariate statistics object whose ndistinct data should be loaded
- : Boolean flag indicating whether to load statistics for inheritance hierarchies (true) or just the specific relation (false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md): Searches system cache for statistics data
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md): Retrieves specific attribute from cached tuple
  - [statext_ndistinct_deserialize](statext_ndistinct_deserialize.md): Converts binary data back to MVNDistinct structure
  - DatumGetByteaPP: Converts Datum to bytea pointer
  - HeapTupleIsValid: Validates heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases system cache reference
- Called from (representative examples):
  - [estimate_multivariate_ndistinct](../e/estimate_multivariate_ndistinct.md): Uses loaded statistics for selectivity estimation

## Notes and Other Information
- Uses STATEXTDATASTXOID cache for efficient lookup of statistics data
- Checks for STATS_EXT_NDISTINCT kind specifically in error messages
- Raises ERROR if statistics object is not found or ndistinct statistics not built
- Properly manages system cache resources by releasing the tuple after use
- Part of the query planner's infrastructure for using multivariate statistics in cost estimation
- The inh parameter allows handling of partitioned tables and inheritance hierarchies

## Simplified Source

```c
MVNDistinct *
statext_ndistinct_load(Oid mvoid, bool inh)
{
    MVNDistinct *result;
    bool isnull;
    Datum ndist;
    HeapTuple htup;

    // Look up the statistics object in the system cache
    htup = SearchSysCache2(STATEXTDATASTXOID,
                          ObjectIdGetDatum(mvoid), BoolGetDatum(inh));
    if (!HeapTupleIsValid(htup))
        elog(ERROR, "cache lookup failed for statistics object %u", mvoid);

    // Extract the ndistinct data from the tuple
    ndist = SysCacheGetAttr(STATEXTDATASTXOID, htup,
                           Anum_pg_statistic_ext_data_stxdndistinct, &isnull);
    if (isnull)
        elog(ERROR,
             "requested statistics kind \"%c\" is not yet built for statistics object %u",
             STATS_EXT_NDISTINCT, mvoid);

    // Deserialize the binary data into MVNDistinct structure
    result = statext_ndistinct_deserialize(DatumGetByteaPP(ndist));

    // Clean up the cache reference
    ReleaseSysCache(htup);

    return result;
}
```