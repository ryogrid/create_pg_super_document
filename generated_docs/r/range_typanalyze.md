# range_typanalyze

## Location
[src/backend/utils/adt/rangetypes_typanalyze.c:46-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_typanalyze.c#L46-L71)

## Overview
The  function is a specialized type analysis function for range columns in PostgreSQL, used during the ANALYZE command to set up statistics collection for range data types.

## Definition

```c
Datum
range_typanalyze(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the type-specific analysis setup routine for range columns. It is called during the ANALYZE process to configure how statistics should be collected for range data types. The function sets up the necessary parameters for statistics collection, including the computation function and minimum sample size requirements.

The function retrieves type cache information for the range type (handling domains appropriately), sets the statistics target if not already specified, and configures the statistics collection framework to use the specialized range statistics computation function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (VacAttrStats *): Pointer to the statistics collection structure for the column being analyzed

## Dependencies
- Functions called/Symbols referenced:
  -  (structure for statistics collection parameters)
  -  (retrieves type cache entry for range types)
  -  (resolves base type, handling domains)
  -  (the actual statistics computation function for ranges)
- Called from:
  - No direct references found (likely registered as a type analysis function)

## Notes and Other Information
- Sets the default statistics target to  if not explicitly specified
- Configures minimum sample size to 300 times the statistics target, following the same pattern as standard type analysis
- Uses the type cache system to efficiently handle range type information
- Handles domain types by resolving to their base range type
- The function always returns , indicating successful setup of the analysis parameters

## Simplified Source

```c
Datum range_typanalyze(PG_FUNCTION_ARGS)
{
    VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);

    // Get type cache for range type (handles domains)
    TypeCacheEntry *typcache = range_get_typcache(fcinfo, getBaseType(stats->attrtypid));

    // Set default statistics target if not specified
    if (stats->attstattarget < 0)
        stats->attstattarget = default_statistics_target;

    // Configure statistics computation for ranges
    stats->compute_stats = compute_range_stats;
    stats->extra_data = typcache;
    stats->minrows = 300 * stats->attstattarget;

    PG_RETURN_BOOL(true);
}
```