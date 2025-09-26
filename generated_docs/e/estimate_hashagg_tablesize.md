# estimate_hashagg_tablesize

## Location
src/backend/utils/adt/selfuncs.c: 3930 - 3966

## Overview
Estimates the total memory size in bytes required for a hash aggregate hashtable based on aggregation costs, path width, and the expected number of groups.

## Definition


## Detailed Description
This function calculates the estimated memory footprint of a hash aggregation hashtable by multiplying the per-entry size by the expected number of groups. The calculation uses the  function to determine the size of each hash table entry based on:

- The number of aggregate transition states
- The width of the path's target list  
- The transition space required by aggregate functions

The function returns a double value to prevent potential integer overflow when multiplying by large group counts. The calculation intentionally disregards hash table fill-factor and growth policies, assuming the default relatively high fill-factor is adequate for estimation purposes.

## Parameters
- : PlannerInfo structure containing query planning context and aggregate transition info
- : The Path node containing the target list and its width
- : Structure containing aggregate clause costs including transition space requirements
- : Expected number of distinct groups in the aggregation result

## Dependencies
- Functions called:
  - hash_agg_entry_size
- Called from:
  - consider_groupingsets_paths (in planner.c:4277, 4411, 4463)

## Notes and Other Information
- The function may over-estimate table size since hash aggregation now omits unneeded columns from the hashtable
- For mixed-mode grouping sets, grouping columns not in the hashed set are counted even though they won't be stored
- Hash table growth policies and fill-factors are not considered in the estimation
- Returns double precision to handle potentially large memory size calculations
- Used primarily for deciding between hash-based and sort-based aggregation strategies