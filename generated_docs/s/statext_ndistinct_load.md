# statext_ndistinct_load

## Location
src/backend/statistics/mvdistinct.c: 148 - 178

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
  - SearchSysCache2: Searches system cache for statistics data
  - SysCacheGetAttr: Retrieves specific attribute from cached tuple
  - statext_ndistinct_deserialize: Converts binary data back to MVNDistinct structure
  - DatumGetByteaPP: Converts Datum to bytea pointer
  - HeapTupleIsValid: Validates heap tuple
  - ReleaseSysCache: Releases system cache reference
- Called from (representative examples):
  - estimate_multivariate_ndistinct: Uses loaded statistics for selectivity estimation

## Notes and Other Information
- Uses STATEXTDATASTXOID cache for efficient lookup of statistics data
- Checks for STATS_EXT_NDISTINCT kind specifically in error messages
- Raises ERROR if statistics object is not found or ndistinct statistics not built
- Properly manages system cache resources by releasing the tuple after use
- Part of the query planner's infrastructure for using multivariate statistics in cost estimation
- The inh parameter allows handling of partitioned tables and inheritance hierarchies