# multirange_typanalyze

## Location
[src/backend/utils/adt/rangetypes_typanalyze.c:72-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_typanalyze.c#L72-L94)

## Overview
The  function is a specialized type analysis function for multirange columns in PostgreSQL, used during the ANALYZE command to set up statistics collection for multirange data types.

## Definition


## Detailed Description
This function serves as the type-specific analysis setup routine for multirange columns. It performs the same analysis approach as range types, but operates on the smallest range that completely includes the multirange. The function configures the statistics collection framework to analyze multirange data by treating it as equivalent to a single encompassing range.

Like its range counterpart, this function retrieves type cache information for the multirange type (handling domains appropriately), sets the statistics target if not specified, and configures the system to use the specialized range statistics computation function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (VacAttrStats *): Pointer to the statistics collection structure for the column being analyzed

## Dependencies
- Functions called/Symbols referenced:
  -  (structure for statistics collection parameters)
  -  (retrieves type cache entry for multirange types)
  -  (resolves base type, handling domains)
  -  (the statistics computation function, shared with range types)
- Called from:
  - No direct references found (likely registered as a type analysis function)

## Notes and Other Information
- Uses the same statistics computation strategy as regular ranges by analyzing the bounding range
- Reuses the  function, demonstrating code sharing between range and multirange analysis
- Sets the default statistics target to  if not explicitly specified
- Configures minimum sample size to 300 times the statistics target, following standard conventions
- Handles domain types by resolving to their base multirange type
- The function always returns , indicating successful setup of the analysis parameters
- The approach of analyzing multiranges as their bounding range provides efficient statistics while maintaining compatibility with existing range analysis infrastructure