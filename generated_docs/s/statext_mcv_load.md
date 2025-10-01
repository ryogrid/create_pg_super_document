# statext_mcv_load

## Location
[src/backend/statistics/mcv.c:558-620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L558-L620)

## Overview
Loads and deserializes an MCV (Most Common Values) list from the PostgreSQL system catalog for a given statistics object.

## Definition
```c
MCVList *statext_mcv_load(Oid mvoid, bool inh)
```

## Detailed Description
This function retrieves MCV statistics data from the pg_statistic_ext_data system catalog and deserializes it into an MCVList structure. It performs a cache lookup using the statistics object OID and inheritance flag, validates that the requested MCV statistics exist, and converts the stored bytea data back into the in-memory MCVList format. The function handles error cases where the statistics object doesn't exist or the MCV data hasn't been built yet.

## Parameters / Member Variables
- `mvoid`: Object ID (OID) of the statistics object to load MCV data for
- `inh`: Boolean flag indicating whether to load inherited statistics (for partitioned tables)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [statext_mcv_deserialize](statext_mcv_deserialize.md)
  - DatumGetByteaP
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - HeapTupleIsValid
  - STATS_EXT_MCV
- Called from (representative examples):
  - [statext_mcv_clauselist_selectivity](statext_mcv_clauselist_selectivity.md)
  - [mcv_clauselist_selectivity](../m/mcv_clauselist_selectivity.md)

## Notes and Other Information
- Part of PostgreSQL's extended statistics system for query planning
- Validates statistics existence and raises errors for missing data
- Uses system cache for efficient catalog access
- The inheritance flag supports partitioned table statistics
- Returns a fully deserialized MCVList ready for selectivity estimation
- Caller is responsible for managing the returned MCVList memory

## Simplified Source

```c
MCVList *statext_mcv_load(Oid mvoid, bool inh) {
    // Look up statistics object in system catalog
    HeapTuple htup = SearchSysCache2(STATEXTDATASTXOID,
                                    ObjectIdGetDatum(mvoid),
                                    BoolGetDatum(inh));

    // Validate statistics object exists
    if (!HeapTupleIsValid(htup))
        elog(ERROR, "cache lookup failed for statistics object %u", mvoid);

    // Extract MCV data from tuple
    bool isnull;
    Datum mcvlist = SysCacheGetAttr(STATEXTDATASTXOID, htup,
                                   Anum_pg_statistic_ext_data_stxdmcv, &isnull);

    // Ensure MCV statistics have been built
    if (isnull)
        elog(ERROR, "MCV statistics not built for object %u", mvoid);

    // Deserialize bytea data into MCVList structure
    MCVList *result = statext_mcv_deserialize(DatumGetByteaP(mcvlist));

    ReleaseSysCache(htup);
    return result;
}
```