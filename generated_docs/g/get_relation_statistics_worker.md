# get_relation_statistics_worker

## Location
[src/backend/optimizer/util/plancat.c:1387-1469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1387-L1469)

## Overview
Internal worker function that loads and creates StatisticExtInfo structures for available extended statistics data of a specific statistics object.

## Definition


## Detailed Description
The  function is responsible for loading extended statistics data for a specific statistics object identified by its OID and inheritance flag. It searches the system cache for the statistics data and creates  structures for each type of extended statistics that has been built and is available.

The function supports four types of extended statistics: NDISTINCT (n-distinct estimates), DEPENDENCIES (functional dependencies), MCV (most common values), and EXPRESSIONS (statistics on expressions). For each available statistics type, it creates a corresponding  structure with the appropriate metadata and adds it to the provided  list.

The function handles the case where the requested statistics data may not exist by checking the validity of the cache lookup result and returning early if no data is found.

## Parameters / Member Variables
- : Pointer to list of StatisticExtInfo structures to append results to
- : RelOptInfo structure representing the relation in the optimizer
- : OID of the statistics object to load data for
- : Whether to include inherited statistics data
- : Bitmapset of attribute numbers covered by the statistics object
- : List of expressions covered by the statistics object

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_statistic_ext_data
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [statext_is_kind_built](../s/statext_is_kind_built.md)
  - StatisticExtInfo
  - [bms_copy](../b/bms_copy.md)
  - STATS_EXT_NDISTINCT
  - STATS_EXT_DEPENDENCIES
  - STATS_EXT_MCV
  - STATS_EXT_EXPRESSIONS
- Called from (representative examples):
  - [get_relation_statistics](get_relation_statistics.md)

## Notes and Other Information
- This is a static helper function, not part of the external API
- Returns early if the requested statistics data does not exist in the system cache
- Creates separate StatisticExtInfo entries for each available statistics type
- Uses bms_copy() to create independent copies of the keys bitmapset for each info structure
- The inherit flag from the statistics data overrides the inh parameter in the resulting StatisticExtInfo
- Properly manages system cache resources by calling ReleaseSysCache()