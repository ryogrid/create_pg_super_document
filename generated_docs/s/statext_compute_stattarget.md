# statext_compute_stattarget

## Location
src/backend/statistics/extended_stats.c: 347 - 388

## Overview
statext_compute_stattarget computes the effective statistics target for an extended statistics object by considering the object's own target, individual attribute targets, and system defaults in a hierarchical fashion.

## Definition


## Detailed Description
This function implements a three-tier hierarchy for determining the statistics target of extended statistics objects. It first checks if the statistics object itself has an explicit target set via ALTER STATISTICS ... SET STATISTICS (any non-negative value including 0). If not found, it examines the statistics targets of all individual attributes covered by the statistics object and uses the maximum value found. Finally, if no explicit targets are found at either level, it falls back to the system default_statistics_target. The function ensures backwards compatibility with the pre-extended-statistics behavior while supporting the newer object-level target setting.

## Parameters / Member Variables
- : The statistics target set on the extended statistics object itself (-1 if not set)
- : Number of attributes covered by the statistics object
- : Array of VacAttrStats structures for the attributes

## Dependencies
- Functions called/Symbols referenced:
  - default_statistics_target (global variable)
  - MAX_STATISTICS_TARGET (constant)
  - Assert (macro)
- Called from:
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (in src/backend/statistics/extended_stats.c:185)
  - [ComputeExtStatisticsRows](../C/ComputeExtStatisticsRows.md) (in src/backend/statistics/extended_stats.c:309)

## Notes and Other Information
- Returns the object's own target if stattarget >= 0, including 0 which disables statistics
- Uses maximum attribute target when object target is -1 (default)
- Falls back to default_statistics_target if all targets are unset  
- Validates final result with Assert to ensure it's within valid range [0, MAX_STATISTICS_TARGET]
- Maintains backwards compatibility by honoring per-column statistics targets
- A target of 0 disables building the statistics object entirely