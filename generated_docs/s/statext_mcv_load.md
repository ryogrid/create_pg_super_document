# statext_mcv_load

## Location
src/backend/statistics/mcv.c: 558 - 620

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
  - SearchSysCache2
  - SysCacheGetAttr
  - statext_mcv_deserialize
  - DatumGetByteaP
  - ObjectIdGetDatum
  - BoolGetDatum
  - ReleaseSysCache
  - HeapTupleIsValid
  - STATS_EXT_MCV
- Called from (representative examples):
  - statext_mcv_clauselist_selectivity
  - mcv_clauselist_selectivity

## Notes and Other Information
- Part of PostgreSQL's extended statistics system for query planning
- Validates statistics existence and raises errors for missing data
- Uses system cache for efficient catalog access
- The inheritance flag supports partitioned table statistics
- Returns a fully deserialized MCVList ready for selectivity estimation
- Caller is responsible for managing the returned MCVList memory