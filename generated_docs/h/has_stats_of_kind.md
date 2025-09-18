# has_stats_of_kind

## Location
src/backend/statistics/extended_stats.c: 1118 - 1140

## Overview
Checks whether a list of extended statistics contains a statistic of a specific kind.

## Definition


## Detailed Description
This utility function iterates through a list of StatisticExtInfo structures to determine if any of them matches a specified statistic kind. It provides a simple boolean check that's commonly used in extended statistics processing to verify the availability of specific types of statistics (such as dependency, MCV, or other extended statistic kinds) before attempting to use them for query planning or selectivity estimation.

## Parameters / Member Variables
- : List of StatisticExtInfo structures representing available extended statistics
- : Character code representing the type of statistic to search for

## Dependencies
- Functions called/Symbols referenced:
  - StatisticExtInfo (type)
  - List (PostgreSQL list operations)
  - lfirst (list traversal macro)
- Called from (representative examples):
  - dependencies_clauselist_selectivity
  - statext_mcv_clauselist_selectivity

## Notes and Other Information
- Returns true immediately upon finding the first matching statistic kind
- Returns false if no matching statistic kind is found in the entire list
- Used primarily in selectivity estimation functions to verify statistic availability
- Part of the extended statistics framework for multi-column statistics support
- Simple linear search implementation suitable for typical small statistic lists
- Located in src/backend/statistics/extended_stats.c:1118-1140